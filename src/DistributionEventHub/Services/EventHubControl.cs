using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Represents the event hub control
/// </summary>
internal class EventHubControl(IBusControl busControl) : IEventHubControl
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
        var handle = busControl.ConnectReceiveEndpoint(destinationAddress,
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
        var handle = busControl.ConnectReceiveEndpoint(typeof(TMessage).FullName ?? "Unknown",
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
        
        var handle = busControl.ConnectReceiveEndpoint(commandName,
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