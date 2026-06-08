using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Represents the event hub control
/// </summary>
internal class EventHubControl(IBusControl busControl, IBroadcastServiceAddress serviceAddress) : IEventHubControl
{
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        return busControl.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        return busControl.StopAsync(cancellationToken);
    }

    public EndpointHandle RegisterRoutedEventConsumer<TMessage>(string destinationAddress, Func<TMessage, Task> handler)
        where TMessage : class
    {
        var prefixedDestinationAddress = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, destinationAddress);

        var handle = busControl.ConnectReceiveEndpoint(prefixedDestinationAddress,
            e =>
            {
                if (e is IRabbitMqReceiveEndpointConfigurator rabbitConfigurator)
                {
                    // Never exclusive: a connection-locked queue cannot be re-declared on
                    // reconnect/redeploy/instance-overlap and turns transient blips into
                    // RESOURCE_LOCKED retry loops that mask the real fault.
                    rabbitConfigurator.Exclusive = false;
                }

                e.Handler<TMessage>(async consumeContext =>
                {
                    await handler(consumeContext.Message).ConfigureAwait(false);
                });
            });

        return new EndpointHandle(handle);
    }
    
    public EndpointHandle RegisterRoutedEventConsumer<TMessage>(Func<TMessage, Task> handler)
        where TMessage : class
    {
        var queueName = busControl.Topology.Message<TMessage>().EntityNameFormatter.FormatEntityName();
        var handle = busControl.ConnectReceiveEndpoint(queueName,
            e =>
            {
                if (e is IRabbitMqReceiveEndpointConfigurator rabbitConfigurator)
                {
                    rabbitConfigurator.Exclusive = false;
                }

                e.Handler<TMessage>(async consumeContext =>
                {
                    await handler(consumeContext.Message).ConfigureAwait(false);
                });
            });
    
        return new EndpointHandle(handle);
    }
    
    public EndpointHandle RegisterRoutedEventConsumer<TMessage>(string exchangeName, string routingKey,
        Func<TMessage, Task> handler) where TMessage : class
    {
        var prefixedExchangeName = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, exchangeName);
        var sanitizedRoutingKey = routingKey.Replace(".", "-").Replace("#", "_").Replace("*", "_").Replace("/", "-").Replace("@", "-");
        var guidSuffix = Guid.NewGuid().ToString("N");
        var queueName = $"{prefixedExchangeName}-{sanitizedRoutingKey}-{guidSuffix}";
        // RabbitMQ queue names are limited to 255 bytes.
        if (queueName.Length > 255)
        {
            throw new ArgumentException(
                $"Resulting queue name exceeds the RabbitMQ limit of 255 characters " +
                $"(got {queueName.Length}). Shorten the exchange name or routing key.",
                nameof(routingKey));
        }

        var handle = busControl.ConnectReceiveEndpoint(queueName,
            e =>
            {
                e.ConfigureConsumeTopology = false;

                if (e is IRabbitMqReceiveEndpointConfigurator rabbitConfigurator)
                {
                    // Durable=true is required to keep the queue non-exclusive: MassTransit forces a queue
                    // exclusive whenever (AutoDelete && !Durable), and an exclusive queue cannot be
                    // re-declared by the same endpoint after a transport reconnect until the broker reaps
                    // the previous owner — exactly the RESOURCE_LOCKED loop seen on these per-subscriber
                    // (GUID-named) topic queues. AutoDelete stays true so the queue is removed once its
                    // consumer goes away. No single-active-consumer here: this is pub/sub fan-out, every
                    // subscriber instance gets its own copy.
                    rabbitConfigurator.AutoDelete = true;
                    rabbitConfigurator.Durable = true;
                    rabbitConfigurator.Exclusive = false;

                    rabbitConfigurator.Bind(prefixedExchangeName, bindConfig =>
                    {
                        bindConfig.RoutingKey = routingKey;
                        bindConfig.ExchangeType = "topic";
                    });
                }

                e.Handler<TMessage>(async consumeContext =>
                {
                    await handler(consumeContext.Message).ConfigureAwait(false);
                });
            });

        return new EndpointHandle(handle);
    }

    public EndpointHandle RegisterCommandConsumer<TMessage>(string commandName, ExecuteCommandHandler<TMessage> handler)
        where TMessage : class
    {
        var prefixedCommandName = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, commandName);
        var handle = busControl.ConnectReceiveEndpoint(prefixedCommandName,
            e =>
            {
                if (e is IRabbitMqReceiveEndpointConfigurator rabbitConfigurator)
                {
                    // MassTransit's topology builder forces a queue exclusive whenever
                    // (AutoDelete && !Durable) — so the previous AutoDelete=true/Durable=false combo
                    // declared this queue exclusive regardless of Exclusive=false, which is what caused
                    // the chronic RESOURCE_LOCKED storm: on every reconnect the fresh connection could
                    // not re-declare the queue still owned by the not-yet-reaped previous connection.
                    // Dropping AutoDelete keeps the queue non-exclusive (re-declarable on reconnect) and
                    // lets it survive the brief reconnect gap, buffering in-flight requests instead of
                    // losing them. The sender exchange flags must match (durable=false, autodelete=false)
                    // — see DistributionEventHubService and RoutedCommandClient.
                    rabbitConfigurator.Durable = false;
                    rabbitConfigurator.AutoDelete = false;
                    rabbitConfigurator.Exclusive = false;
                    // Single-active-consumer preserves the "exactly one adapter handles this command"
                    // guarantee on a shared (non-exclusive) queue: the reconnecting consumer attaches
                    // immediately and the broker promotes it once the old owner drains — clean failover
                    // instead of a lock-retry storm.
                    rabbitConfigurator.SetQueueArgument("x-single-active-consumer", true);
                }

                e.Handler<TMessage>(async consumeContext =>
                {
                    async Task RespondToCommand(object response)
                    {
                        await consumeContext.RespondAsync(response).ConfigureAwait(false);
                    }

                    await handler(consumeContext.Message, RespondToCommand).ConfigureAwait(false);
                });
            });

        return new EndpointHandle(handle);
    }
}