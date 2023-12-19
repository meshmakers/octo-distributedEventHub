namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Interface of client to distribute events directly or by broadcast
/// </summary>
public interface IDistributionEventHubService
{
    /// <summary>
    /// Publish an event
    /// </summary>
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(T message, CancellationToken? cancellationToken = null)
        where T : class;
}