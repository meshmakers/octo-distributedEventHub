namespace Meshmakers.Octo.Common.DistributionEventHub.Commands;

/// <summary>
/// Interface of a command client
/// </summary>
/// <typeparam name="TRequest">Type of request</typeparam>
public interface ICommandClient<in TRequest> where TRequest : class
{
    /// <summary>
    /// Create a request, and return a task for the specified response type
    /// </summary>
    /// <param name="message">The request message</param>
    /// <param name="cancellationToken">An optional cancellationToken to cancel the request</param>
    /// <param name="timeout">
    /// An optional timeout, to automatically cancel the request after the specified timeout period
    /// </param>
    /// <typeparam name="TResponse"></typeparam>
    /// <returns></returns>
    Task<TResponse> GetResponse<TResponse>(TRequest message, CancellationToken cancellationToken = default,
        TimeSpan? timeout = default)
        where TResponse : class;

    
}