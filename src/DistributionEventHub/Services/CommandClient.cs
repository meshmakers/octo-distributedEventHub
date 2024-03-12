using MassTransit;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Implements the <see cref="ICommandClient{TRequest}" /> interface.
/// </summary>
/// <typeparam name="TRequest"></typeparam>
internal class CommandClient<TRequest> : ICommandClient<TRequest> where TRequest : class
{
    private readonly IRequestClient<TRequest> _requestClient;
    private readonly ILogger<CommandClient<TRequest>> _logger;

    /// <summary>
    ///     Configures the request client
    /// </summary>
    /// <param name="requestClient"></param>
    /// <param name="logger"></param>
    public CommandClient(IRequestClient<TRequest> requestClient, ILogger<CommandClient<TRequest>> logger)
    {
        _requestClient = requestClient;
        _logger = logger;
    }

    public async Task<TResponse> GetResponse<TResponse>(TRequest message, CancellationToken cancellationToken = default,
        TimeSpan? timeout = default) where TResponse : class
    {
        var requestTimeout = timeout ?? RequestTimeout.Default;
        try
        {
            var response = await _requestClient.GetResponse<TResponse>(message, cancellationToken, requestTimeout)
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

    public async Task<TResponse> GetResponseWithRetry<TResponse>(TRequest message, int retryCount = 5,
        TimeSpan? retryWaitTime = null, CancellationToken cancellationToken = default,
        TimeSpan? timeout = default) where TResponse : class
    {
        for (int i = 0; i < retryCount; i++)
        {
            try
            {
                return await GetResponse<TResponse>(message, cancellationToken, timeout).ConfigureAwait(false);
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