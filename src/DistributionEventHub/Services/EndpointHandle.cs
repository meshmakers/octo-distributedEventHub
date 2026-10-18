using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Wraps a MassTransit endpoint handle with a timeout on dispose to prevent
/// indefinite hangs when in-flight consumers are stuck.
/// </summary>
public class EndpointHandle : IAsyncDisposable
{
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(30);

    private readonly HostReceiveEndpointHandle _handle;
    private readonly IAsyncDisposable? _companion;

    /// <summary>
    ///     Wraps a MassTransit endpoint handle.
    /// </summary>
    /// <param name="handle">Handle to the endpoint</param>
    public EndpointHandle(HostReceiveEndpointHandle handle) : this(handle, null)
    {
    }

    /// <summary>
    ///     Wraps a MassTransit endpoint handle together with a helper that lives as long as the endpoint
    ///     (the legacy command bridge of a migrated command, AB#6909).
    /// </summary>
    internal EndpointHandle(HostReceiveEndpointHandle handle, IAsyncDisposable? companion)
    {
        _handle = handle;
        _companion = companion;
    }

    /// <summary>
    ///     Completes when the endpoint has started and its queue, exchange and bindings exist on the broker,
    ///     i.e. when messages sent to it are routable. Await it before announcing the endpoint to other services.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait (not the endpoint)</param>
    public async Task WaitUntilReadyAsync(CancellationToken cancellationToken = default)
    {
        await _handle.Ready.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        using var cts = new CancellationTokenSource(StopTimeout);
        try
        {
            await _handle.StopAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Timeout expired waiting for in-flight consumers to complete.
            // Proceed with disposal to avoid blocking the shutdown.
        }
        finally
        {
            if (_companion != null)
            {
                await _companion.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}