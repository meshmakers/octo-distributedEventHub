using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Implements the <see cref="ICommandClient{TRequest}"/> interface. 
/// </summary>
/// <typeparam name="TRequest"></typeparam>
internal class CommandClient<TRequest> : ICommandClient<TRequest> where TRequest : class
{
    private readonly IRequestClient<TRequest> _requestClient;

    /// <summary>
    /// Configures the request client
    /// </summary>
    /// <param name="requestClient"></param>
    public CommandClient(IRequestClient<TRequest> requestClient)
    {
        _requestClient = requestClient;
    }

    public async Task<TResponse> GetResponse<TResponse>(TRequest message, CancellationToken cancellationToken = default, 
        TimeSpan? timeout = default) where TResponse : class
    {
        var requestTimeout = timeout ?? RequestTimeout.Default;
        try
        {
            var response = await _requestClient.GetResponse<TResponse>(message, cancellationToken, requestTimeout).ConfigureAwait(false);
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
}