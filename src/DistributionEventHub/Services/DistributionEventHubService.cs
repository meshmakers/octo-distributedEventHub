using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

internal class DistributionEventHubService : IDistributionEventHubService
{
    private readonly IBus _bus;

    public DistributionEventHubService(IBus bus)
    {
        _bus = bus;
    }
    
    public Task PublishAsync<T>(T message, CancellationToken? cancellationToken = null) where T : class
    {
        return _bus.Publish(message, cancellationToken ?? CancellationToken.None);
    }
}