namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Interface of a command client
/// </summary>
/// <typeparam name="TRequest">Type of request</typeparam>
public interface ICommandClient<in TRequest> where TRequest : class
{
    /// <summary>
    ///     Create a request, and return a task for the specified response type
    /// </summary>
    /// <param name="message">The request message</param>
    /// <param name="cancellationToken">An optional cancellationToken to cancel the request</param>
    /// <param name="timeout">
    ///     An optional timeout, to automatically cancel the request after the specified timeout period
    /// </param>
    /// <typeparam name="TResponse">Type of response</typeparam>
    /// <returns>Task with the response</returns>
    Task<TResponse> GetResponse<TResponse>(TRequest message, CancellationToken cancellationToken = default,
        TimeSpan? timeout = default)
        where TResponse : class;

    /// <summary>
    ///     Create a request, and return a task for the specified response type, with retry if the request times out
    /// </summary>
    /// <param name="message">The request message</param>
    /// <param name="retryCount">Amount of retries, the default is 5</param>
    /// <param name="retryWaitTime">Retry wait time, the default is 30s</param>
    /// <param name="cancellationToken">An optional cancellationToken to cancel the request</param>
    /// <param name="timeout">An optional timeout, to automatically cancel the request after the specified timeout period</param>
    /// <typeparam name="TResponse">Type of response</typeparam>
    /// <returns>Task with the response</returns>
    Task<TResponse> GetResponseWithRetry<TResponse>(TRequest message, int retryCount = 5,
        TimeSpan? retryWaitTime = null, CancellationToken cancellationToken = default,
        TimeSpan? timeout = default) where TResponse : class;
}