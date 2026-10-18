using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Wraps a MassTransit endpoint handle with a timeout on dispose to prevent
/// indefinite hangs when in-flight consumers are stuck.
/// </summary>
/// <param name="handle">Handle to the endpoint</param>
public class EndpointHandle(HostReceiveEndpointHandle handle) : IAsyncDisposable
{
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Completes when the endpoint has started and its queue, exchange and bindings exist on the broker,
    ///     i.e. when messages sent to it are routable. Await it before announcing the endpoint to other services.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait (not the endpoint)</param>
    public async Task WaitUntilReadyAsync(CancellationToken cancellationToken = default)
    {
        await handle.Ready.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        using var cts = new CancellationTokenSource(StopTimeout);
        try
        {
            await handle.StopAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Timeout expired waiting for in-flight consumers to complete.
            // Proceed with disposal to avoid blocking the shutdown.
        }
    }
}