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
            // durable=false&autodelete=false (not temporary): a temporary exchange is auto-delete, and the
            // receive endpoint queue inherits the exchange flags — MassTransit then forces the queue
            // exclusive (AutoDelete && !Durable), which is what caused the RESOURCE_LOCKED storm. These flags
            // must stay in sync with the consumer in EventHubControl.RegisterCommandConsumer.
            var requestClient = bus.CreateRequestClient<TRequest>(new Uri($"exchange:{prefixedCommandAddress}?durable=false&autodelete=false"), requestTimeout);
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
}