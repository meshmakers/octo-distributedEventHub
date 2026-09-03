namespace Meshmakers.Octo.Common.DistributionEventHub;

/// <summary>
/// Represents options for recurring schedules
/// </summary>
public record RecurringSchedulingOptions
{
    /// <summary>
    /// Represents options for recurring schedules
    /// </summary>
    /// <param name="cronExpression">The Cron Schedule Expression in Cron Syntax</param>
    /// <param name="startTime">The time the recurring schedule is enabled</param>
    /// <param name="endTime">The time the recurring schedule is disabled, if null then the job is repeated forever</param>
    /// <param name="scheduleId">A unique name that identifies this schedule.</param>
    /// <param name="scheduleGroup">Schedule group</param>
    /// <param name="description">Schedule description</param>
    /// <param name="misfirePolicy">If the scheduler is offline and comes back online, the policy determines how
    /// a missed scheduled message is handled.</param>
    public RecurringSchedulingOptions(string cronExpression, DateTime startTime, DateTime? endTime, string scheduleId,
        string scheduleGroup, string description,
        SchedulingMissedEventPolicy misfirePolicy = SchedulingMissedEventPolicy.Default)
    {
        CronExpression = cronExpression;
        StartTime = startTime;
        EndTime = endTime;
        ScheduleId = scheduleId;
        ScheduleGroup = scheduleGroup;
        Description = description;
        MisfirePolicy = misfirePolicy;
    }

    /// <summary>
    /// Represents options for recurring schedules
    /// </summary>
    /// <param name="cronExpression">The Cron Schedule Expression in Cron Syntax</param>
    /// <param name="scheduleId">A unique name that identifies this schedule.</param>
    /// <param name="scheduleGroup">Schedule group</param>
    /// <param name="description">Schedule description</param>
    /// <param name="misfirePolicy">If the scheduler is offline and comes back online, the policy determines how
    /// a missed scheduled message is handled.</param>
    public RecurringSchedulingOptions(string cronExpression, string scheduleId, string scheduleGroup,
        string description, SchedulingMissedEventPolicy misfirePolicy = SchedulingMissedEventPolicy.Default)
    {
        CronExpression = cronExpression;
        StartTime = DateTime.Now;
        ScheduleId = scheduleId;
        ScheduleGroup = scheduleGroup;
        Description = description;
        MisfirePolicy = misfirePolicy;
    }

    /// <summary>
    /// The Cron Schedule Expression in Cron Syntax
    /// </summary>
    public string CronExpression { get; init; }

    /// <summary>
    /// The time the recurring schedule is enabled
    /// </summary>
    public DateTime StartTime { get; init; }

    /// <summary>
    /// The time the recurring schedule is disabled
    /// If null then the job is repeated forever
    /// </summary>
    public DateTime? EndTime { get; init; }

    /// <summary>
    /// A unique name that identifies this schedule. 
    /// </summary>
    public string ScheduleId { get; init; }

    /// <summary>
    /// Description of the schedule
    /// </summary>
    public string ScheduleGroup { get; init; }
    /// <summary>
    /// Description of the schedule
    /// </summary>
    public string Description { get; init; }
    /// <summary>
    /// If the scheduler is offline and comes back online, the policy determines how
    /// a missed scheduled message is handled.
    /// </summary>
    public SchedulingMissedEventPolicy MisfirePolicy { get; init; }
}