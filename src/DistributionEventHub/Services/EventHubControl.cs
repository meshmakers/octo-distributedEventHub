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