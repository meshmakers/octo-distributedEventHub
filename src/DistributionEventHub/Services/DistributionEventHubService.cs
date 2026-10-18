using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

internal class DistributionEventHubService(IBus bus, IBroadcastServiceAddress serviceAddress)
    : IDistributionEventHubService
{
    public async Task ScheduleRecurringSendAsync<T>(T message, string destinationQueueAddress,
        RecurringSchedulingOptions recurringSchedulingOptions) where T : class
    {
        var recurringSchedule = new OctoRecurringSchedule(recurringSchedulingOptions);
        var prefixedDestinationQueueAddress =
            CacheCommon.ApplyInstancePrefixToUri(serviceAddress.InstancePrefix, new Uri(destinationQueueAddress));
        
        await bus.ScheduleRecurringSend(prefixedDestinationQueueAddress, recurringSchedule, message)
            .ConfigureAwait(false);
    }

    public async Task CancelScheduledRecurringSendAsync(string scheduleId, string scheduleGroup)
    {
        await bus.CancelScheduledRecurringSend(scheduleId, scheduleGroup).ConfigureAwait(false);
    }

    public async Task PublishAsync<T>(T message, CancellationToken? cancellationToken = null) where T : class
    {
        var prefixedExchangeName = bus.Topology.Message<T>().EntityNameFormatter.FormatEntityName();
        var exchangeUri = new Uri($"exchange:{prefixedExchangeName}");
        var endpoint = await bus.GetSendEndpoint(exchangeUri).ConfigureAwait(false);
        await endpoint.Send(message, cancellationToken ?? CancellationToken.None).ConfigureAwait(false);
    }

    public async Task<Task> SendAsync<T>(Uri address, T message, CancellationToken? cancellationToken = null)
        where T : class
    {
        var endpointAddress = CacheCommon.ApplyInstancePrefixToUri(serviceAddress.InstancePrefix, address);
        var endpoint = await bus.GetSendEndpoint(endpointAddress).ConfigureAwait(false);
        return endpoint.Send(message, cancellationToken ?? CancellationToken.None);
    }

    public async Task SendToExchangeAsync<T>(string exchangeName, string routingKey, T message,
        CancellationToken? cancellationToken = null) where T : class
    {
        var prefixedExchangeName = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, exchangeName);
        var exchangeUri = new Uri($"exchange:{prefixedExchangeName}?type=topic");
        var endpoint = await bus.GetSendEndpoint(exchangeUri).ConfigureAwait(false);
        await endpoint.Send(message, context => { context.SetRoutingKey(routingKey); },
            cancellationToken ?? CancellationToken.None).ConfigureAwait(false);
    }

    public async Task<TResponse> GetCommandResponseAsync<TRequest, TResponse>(string commandAddress, TRequest request,
        CancellationToken cancellationToken = default, TimeSpan? timeout = default)
        where TRequest : class where TResponse : class
    {
        var requestTimeout = timeout ?? RequestTimeout.Default;
        try
        {
            var prefixedCommandAddress = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, commandAddress);
            var requestClient = bus.CreateRequestClient<TRequest>(new Uri($"exchange:{prefixedCommandAddress}?temporary=true"), requestTimeout);
            var response = await requestClient.GetResponse<TResponse>(request, cancellationToken, requestTimeout)
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

    public async Task<TResponse> GetCommandResponseAsync<TRequest, TResponse>(string commandAddress, TRequest request,
        CommandSendOptions options, CancellationToken cancellationToken = default)
        where TRequest : class where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(options);

        RequestTimeout requestTimeout = options.Timeout ?? RequestTimeout.Default;
        var prefixedCommandAddress = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, commandAddress);
        var sendExchangeName = CacheCommon.GetSharedCommandSendExchangeName(prefixedCommandAddress);
        try
        {
            // Stable send exchange of the shared command topology (see EventHubControl): durable, not auto-delete,
            // so it survives the consumer queue and a mandatory send is returned instead of hitting a 404.
            var requestClient = bus.CreateRequestClient<TRequest>(
                new Uri($"exchange:{sendExchangeName}?type=fanout&durable=true&autodelete=false"), requestTimeout);
            var response = await requestClient.GetResponse<TResponse>(request, x => x.UseExecute(ctx =>
                    {
                        if (options.Mandatory && ctx.TryGetPayload<RabbitMqSendContext>(out var rabbitMqSendContext))
                        {
                            rabbitMqSendContext.Mandatory = true;
                        }

                        if (options.TimeToLive is { } timeToLive)
                        {
                            ctx.TimeToLive = timeToLive;
                        }

                        if (!options.Persistent)
                        {
                            ctx.Durable = false;
                        }
                    }), cancellationToken, requestTimeout)
                .ConfigureAwait(false);
            return response.Message;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception e) when (IsMessageReturned(e))
        {
            throw CommandNotRoutableException.Create(typeof(TRequest).Name, sendExchangeName, e);
        }
        catch (RequestTimeoutException e)
        {
            throw DistributionTimeoutException.Create(typeof(TRequest).Name, requestTimeout, e);
        }
        catch (RequestFaultException e)
        {
            throw DistributedOperationFailedException.CreateCommandFailed(typeof(TRequest).Name, e);
        }
        catch (Exception e)
        {
            // includes an OperationCanceledException that was NOT caused by the caller's token (e.g. bus stopping)
            throw DistributedOperationFailedException.CreateCommandFailed(typeof(TRequest).Name, e);
        }
    }

    /// <summary>
    ///     Detects a broker return of a mandatory message (MassTransit <see cref="MessageReturnedException" /> or
    ///     the RabbitMQ.Client <c>PublishReturnException</c>), also when wrapped as an inner exception.
    /// </summary>
    internal static bool IsMessageReturned(Exception exception)
    {
        for (var current = exception; current != null; current = current.InnerException)
        {
            if (current is MessageReturnedException or RabbitMQ.Client.Exceptions.PublishReturnException)
            {
                return true;
            }

            if (current is AggregateException aggregate && aggregate.InnerExceptions.Any(IsMessageReturned))
            {
                return true;
            }
        }

        return false;
    }
}
