using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

internal class DistributionEventHubService(IBus bus) : IDistributionEventHubService
{
    public async Task ScheduleRecurringSendAsync<T>(T message, string destinationQueueAddress,
        RecurringSchedulingOptions recurringSchedulingOptions) where T : class
    {
        var recurringSchedule = new OctoRecurringSchedule(recurringSchedulingOptions);
        await bus.ScheduleRecurringSend(new Uri(destinationQueueAddress), recurringSchedule, message)
            .ConfigureAwait(false);
    }

    public async Task CancelScheduledRecurringSendAsync(string scheduleId, string scheduleGroup)
    {
        await bus.CancelScheduledRecurringSend(scheduleId, scheduleGroup).ConfigureAwait(false);
    }

    public async Task PublishAsync<T>(T message, CancellationToken? cancellationToken = null) where T : class
    {
        var endpoint = await bus.GetPublishSendEndpoint<T>().ConfigureAwait(false);
        await endpoint.Send(message, cancellationToken ?? CancellationToken.None).ConfigureAwait(false);
    }

    public async Task<Task> SendAsync<T>(Uri address, T message, CancellationToken? cancellationToken = null)
        where T : class
    {
        var endpoint = await bus.GetSendEndpoint(address).ConfigureAwait(false);
        return endpoint.Send(message, cancellationToken ?? CancellationToken.None);
    }
}