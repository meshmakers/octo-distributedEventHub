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
}