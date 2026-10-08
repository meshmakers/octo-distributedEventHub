using MassTransit;
using MassTransit.RabbitMqTransport;
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
                e.Handler<TMessage>(async consumeContext =>
                {
                    await handler(consumeContext.Message).ConfigureAwait(false);
                });
            });

        return new EndpointHandle(handle);
    }
    
    public EndpointHandle RegisterRoutedEventConsumer<TMessage>(string destinationAddress,
        Func<TMessage, RoutedEventDeliveryContext, Task> handler, RoutedEventConsumerOptions options)
        where TMessage : class
    {
        options.Validate();

        var prefixedDestinationAddress = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, destinationAddress);
        var coalescer = options.CoalescePendingMessages ? new PendingMessageCoalescer() : null;

        var handle = busControl.ConnectReceiveEndpoint(prefixedDestinationAddress,
            e =>
            {
                // Consumer-side settings only — no queue arguments, so an existing durable queue is reused
                // as declared (AB#5709: redeclaring it with x-max-length etc. would fail with
                // PRECONDITION_FAILED, and the producer side declares it as well).
                if (options.EffectivePrefetchCount is { } prefetchCount &&
                    e is IRabbitMqReceiveEndpointConfigurator rabbitConfigurator)
                {
                    rabbitConfigurator.PrefetchCount = prefetchCount;
                }

                if (options.EffectiveConcurrentMessageLimit is { } concurrentMessageLimit)
                {
                    e.ConcurrentMessageLimit = concurrentMessageLimit;
                }

                e.Handler<TMessage>(async consumeContext =>
                {
                    var deliveryContext = RoutedEventDeliveryContext.None;
                    if (coalescer != null)
                    {
                        var decision = await coalescer
                            .DecideAsync(() => GetPendingMessageCountAsync(consumeContext))
                            .ConfigureAwait(false);
                        if (decision == null)
                        {
                            // Superseded by a newer message already waiting in the queue: acknowledge only.
                            return;
                        }

                        deliveryContext = decision;
                    }

                    await handler(consumeContext.Message, deliveryContext).ConfigureAwait(false);
                });
            });

        return new EndpointHandle(handle);
    }

    /// <summary>
    ///     Ready messages waiting behind the current delivery, from a passive declare of the consumed queue on
    ///     the consuming channel. <c>null</c> when the transport is not RabbitMQ.
    /// </summary>
    private static async Task<uint?> GetPendingMessageCountAsync(ConsumeContext consumeContext)
    {
        var receiveContext = consumeContext.ReceiveContext;
        if (!receiveContext.TryGetPayload<ChannelContext>(out var channelContext) ||
            !receiveContext.TryGetPayload<ReceiveSettings>(out var receiveSettings))
        {
            return null;
        }

        var declareOk = await channelContext
            .QueueDeclarePassive(receiveSettings.QueueName, consumeContext.CancellationToken)
            .ConfigureAwait(false);
        return declareOk.MessageCount;
    }

    public EndpointHandle RegisterRoutedEventConsumer<TMessage>(Func<TMessage, Task> handler)
        where TMessage : class
    {
        var queueName = busControl.Topology.Message<TMessage>().EntityNameFormatter.FormatEntityName();
        var handle = busControl.ConnectReceiveEndpoint(queueName,
            e =>
            {
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
                    rabbitConfigurator.AutoDelete = true;
                    rabbitConfigurator.Durable = false;

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
                    rabbitConfigurator.Durable = false;
                    rabbitConfigurator.AutoDelete = true; 
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