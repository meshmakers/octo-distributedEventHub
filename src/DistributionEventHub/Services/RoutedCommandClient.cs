using MassTransit;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Implements the <see cref="IRoutedCommandClient{TRequest}" /> interface.
/// </summary>
/// <typeparam name="TRequest"></typeparam>
internal class RoutedCommandClient<TRequest> : IRoutedCommandClient<TRequest> where TRequest : class
{
    private readonly IBus _bus;
    private readonly ILogger<RoutedCommandClient<TRequest>> _logger;
    private readonly IBroadcastServiceAddress _serviceAddress;

    /// <summary>
    ///     Configures the command client
    /// </summary>
    /// <param name="bus"></param>
    /// <param name="logger"></param>
    /// <param name="serviceAddress"></param>
    public RoutedCommandClient(IBus bus, ILogger<RoutedCommandClient<TRequest>> logger, IBroadcastServiceAddress serviceAddress)
    {
        _bus = bus;
        _logger = logger;
        _serviceAddress = serviceAddress;
    }
    
    public async Task<TResponse> GetResponse<TResponse>(string commandAddress, TRequest message, CancellationToken cancellationToken = default,
        TimeSpan? timeout = default) where TResponse : class
    {
        var requestTimeout = timeout ?? RequestTimeout.Default;
        try
        {
            var prefixedCommandAddress = CacheCommon.ApplyInstancePrefix(_serviceAddress.InstancePrefix, commandAddress);
            var requestClient = _bus.CreateRequestClient<TRequest>(new Uri($"exchange:{prefixedCommandAddress}?temporary=true"), requestTimeout);
            var response = await requestClient.GetResponse<TResponse>(message, cancellationToken, requestTimeout)
                .ConfigureAwait(false);
            return response.Message;
        }
        catch (RequestTimeoutException e)
        {
            throw DistributionTimeoutException.Create(typeof(TRequest).Name, requestTimeout, e);
        }
        catch (Exception e)
        {
            throw DistributedOperationFailedException.CreateCommandFailed(typeof(TRequest).Name, e);
        }
    }

    public async Task<TResponse> GetResponseWithRetry<TResponse>(string address, TRequest message, int retryCount = 5,
        TimeSpan? retryWaitTime = null, CancellationToken cancellationToken = default,
        TimeSpan? timeout = default) where TResponse : class
    {
        for (int i = 0; i < retryCount; i++)
        {
            try
            {
                return await GetResponse<TResponse>(address, message, cancellationToken, timeout).ConfigureAwait(false);
            }
            catch (DistributionTimeoutException)
            {
                _logger.LogWarning("Request timeout for {RequestType}. Retrying {Retry} of {RetryCount} times",
                    typeof(TRequest).Name, i, retryCount);
            }
        }

        throw new DistributedOperationFailedException(
            $"Request {typeof(TRequest).Name} failed after {retryCount} retries");
    }
}