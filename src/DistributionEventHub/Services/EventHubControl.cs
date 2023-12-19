using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Represents the event hub control
/// </summary>
internal class EventHubControl : IEventHubControl
{
    private readonly IBusControl _busControl;

    public EventHubControl(IBusControl busControl)
    {
        _busControl = busControl;
    }
    
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        return _busControl.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        return _busControl.StopAsync(cancellationToken);
    }
}