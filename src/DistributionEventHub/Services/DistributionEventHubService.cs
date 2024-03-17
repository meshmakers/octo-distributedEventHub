using MassTransit;
using MassTransit.Scheduling;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

internal class OctoRecurringSchedule(RecurringSchedulingOptions recurringSchedulingOptions) : RecurringSchedule
{
    public string TimeZoneId { get; } = TimeZoneInfo.Local.Id;
    public DateTimeOffset StartTime { get; } = recurringSchedulingOptions.StartTime;
    public DateTimeOffset? EndTime { get; } = recurringSchedulingOptions.EndTime;
    public string ScheduleId { get; } = recurringSchedulingOptions.ScheduleId;
    public string ScheduleGroup { get; } = recurringSchedulingOptions.ScheduleGroup;
    public string CronExpression { get; } = recurringSchedulingOptions.CronExpression;
    public string Description { get; } = recurringSchedulingOptions.Description;
    public MissedEventPolicy MisfirePolicy { get; } = (MissedEventPolicy) recurringSchedulingOptions.MisfirePolicy;
}

internal class DistributionEventHubService(IBus bus) : IDistributionEventHubService
{
    public async Task ScheduleRecurringSendAsync<T>(T message, string destinationQueueAddress, RecurringSchedulingOptions recurringSchedulingOptions) where T : class
    {
        var recurringSchedule = new OctoRecurringSchedule(recurringSchedulingOptions);
        await bus.ScheduleRecurringSend(new Uri(destinationQueueAddress), recurringSchedule, message).ConfigureAwait(false);
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

    public async Task<Task> SendAsync<T>(Uri address, T message, CancellationToken? cancellationToken = null) where T : class
    {
        var endpoint = await bus.GetSendEndpoint(address).ConfigureAwait(false);
        return endpoint.Send(message, cancellationToken ?? CancellationToken.None);
    }
}