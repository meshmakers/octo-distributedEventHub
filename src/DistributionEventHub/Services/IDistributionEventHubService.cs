namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Interface of client to distribute events directly or by broadcast
/// </summary>
public interface IDistributionEventHubService
{
    /// <summary>
    ///     Publish an event
    /// </summary>
    /// <param name="message">Message to publish</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <typeparam name="T">Type of message</typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(T message, CancellationToken? cancellationToken = null)
        where T : class;

    /// <summary>
    ///     Send an event to a specific address
    /// </summary>
    /// <param name="address">Address to send to</param>
    /// <param name="message">Message to publish</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <typeparam name="T">Type of message</typeparam>
    /// <returns></returns>
    Task<Task> SendAsync<T>(Uri address, T message, CancellationToken? cancellationToken = null) where T : class;
}