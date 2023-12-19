namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Interface for event hub control, that allows to start and stop the event hub
/// Especially useful for console apps or tests
/// </summary>
public interface IEventHubControl
{
    /// <summary>
    /// Starts the event hub
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns></returns>
    Task StartAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Stops the event hub
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns></returns>
    Task StopAsync(CancellationToken cancellationToken = default);
}