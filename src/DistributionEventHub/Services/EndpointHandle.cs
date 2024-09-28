using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// 
/// </summary>
/// <param name="handle">Handle to the endpoint</param>
public class EndpointHandle(HostReceiveEndpointHandle handle) : IAsyncDisposable
{
    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await handle.StopAsync().ConfigureAwait(false);
    }
}