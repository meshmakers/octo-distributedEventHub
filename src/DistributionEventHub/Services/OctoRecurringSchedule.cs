using MassTransit.Scheduling;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

internal class OctoRecurringSchedule(RecurringSchedulingOptions recurringSchedulingOptions) : RecurringSchedule
{
    public string TimeZoneId { get; } = ResolveTimeZoneId(recurringSchedulingOptions);
    public DateTimeOffset StartTime { get; } = recurringSchedulingOptions.StartTime;
    public DateTimeOffset? EndTime { get; } = recurringSchedulingOptions.EndTime;
    public string ScheduleId { get; } = recurringSchedulingOptions.ScheduleId;
    public string ScheduleGroup { get; } = recurringSchedulingOptions.ScheduleGroup;
    public string CronExpression { get; } = recurringSchedulingOptions.CronExpression;
    public string Description { get; } = recurringSchedulingOptions.Description;
    public MissedEventPolicy MisfirePolicy { get; } = (MissedEventPolicy) recurringSchedulingOptions.MisfirePolicy;

    private static string ResolveTimeZoneId(RecurringSchedulingOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.TimeZoneId))
        {
            return TimeZoneInfo.Local.Id;
        }

        if (!TimeZoneInfo.TryFindSystemTimeZoneById(options.TimeZoneId, out _))
        {
            throw new ArgumentException(
                $"Unknown time zone '{options.TimeZoneId}' for recurring schedule '{options.ScheduleId}'.",
                nameof(options));
        }

        return options.TimeZoneId;
    }
}