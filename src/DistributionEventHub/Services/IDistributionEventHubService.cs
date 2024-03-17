namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Interface of client to distribute events directly or by broadcast
/// </summary>
public interface IDistributionEventHubService
{
    /// <summary>
    ///     Publish an event
    /// </summary>
    /// <param name="message">Message to publish</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <typeparam name="T">Type of message</typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(T message, CancellationToken? cancellationToken = null)
        where T : class;

    /// <summary>
    ///     Send an event to a specific address
    /// </summary>
    /// <param name="address">Address to send to</param>
    /// <param name="message">Message to publish</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <typeparam name="T">Type of message</typeparam>
    /// <returns></returns>
    Task<Task> SendAsync<T>(Uri address, T message, CancellationToken? cancellationToken = null) where T : class;

    /// <summary>
    ///     Schedule a recurring send
    /// </summary>
    /// <param name="message">Message to send</param>
    /// <param name="destinationQueueAddress">The destination address where the schedule message should be sent</param>
    /// <param name="recurringSchedulingOptions">Options to describe the recurring schedule</param>
    /// <typeparam name="T">Type of message</typeparam>
    /// <returns></returns>
    Task ScheduleRecurringSendAsync<T>(T message, string destinationQueueAddress, RecurringSchedulingOptions recurringSchedulingOptions) where T : class;

    /// <summary>
    ///     Cancel a scheduled recurring send
    /// </summary>
    /// <param name="scheduleId">Unique identifier of the schedule</param>
    /// <param name="scheduleGroup">Group of the schedule</param>
    /// <returns></returns>
    Task CancelScheduledRecurringSendAsync(string scheduleId, string scheduleGroup);
}