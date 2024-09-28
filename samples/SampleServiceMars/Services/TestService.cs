using Meshmakers.Octo.Common.DistributionEventHub.Services;
using SampleEvents.Messages;

namespace SampleServiceMars.Services;

public class TestService(ILogger<TestService> logger, IEventHubControl eventHubControl) : IHostedService 
{
    private EndpointHandle? _routedEventEndpointHandle;
    private EndpointHandle? _commandHandle;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _routedEventEndpointHandle = eventHubControl.RegisterRoutedEventConsumer<UpdateEvent>(message =>
        {
            logger.LogInformation("Update event received: {Name} {DateTime} {Value}", message.Name,
                message.DateTime, message.Value);
            logger.LogInformation("Received message {Value}", message.Value);

            return Task.CompletedTask;
        });
        
        
        _commandHandle = eventHubControl.RegisterCommandConsumer<SampleCommandRequest>("SampleCommandRequest", async (message, response) => 
        {
            logger.LogInformation("Command received: {Value}", message.Value);
            await response(new SampleCommandResponse { Value = message.Value });
        });

        
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_routedEventEndpointHandle != null)
        {
            await _routedEventEndpointHandle.DisposeAsync();
        }
        
        if (_commandHandle != null)
        {
            await _commandHandle.DisposeAsync();
        }
    }
}