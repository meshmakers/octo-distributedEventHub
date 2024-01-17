using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

internal class DistributionEventHubService : IDistributionEventHubService
{
    private readonly IBus _bus;

    public DistributionEventHubService(IBus bus)
    {
        _bus = bus;
    }

    public async Task PublishAsync<T>(T message, CancellationToken? cancellationToken = null) where T : class
    {
        var endpoint = await _bus.GetPublishSendEndpoint<T>().ConfigureAwait(false);
        await endpoint.Send(message, cancellationToken ?? CancellationToken.None).ConfigureAwait(false);
    }

    public async Task<Task> SendAsync<T>(Uri address, T message, CancellationToken? cancellationToken = null) where T : class
    {
        var endpoint = await _bus.GetSendEndpoint(address).ConfigureAwait(false);
        return endpoint.Send(message, cancellationToken ?? CancellationToken.None);
    }
}