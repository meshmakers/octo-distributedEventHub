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

    /// <summary>
    ///     Send a message to a topic exchange with a specific routing key
    /// </summary>
    /// <param name="exchangeName">Name of the topic exchange</param>
    /// <param name="routingKey">Routing key for the message</param>
    /// <param name="message">Message to send</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <typeparam name="T">Type of message</typeparam>
    /// <returns></returns>
    Task SendToExchangeAsync<T>(string exchangeName, string routingKey, T message,
        CancellationToken? cancellationToken = null) where T : class;

    /// <summary>
    ///     Send a command to a specific address and await the response
    /// </summary>
    /// <param name="commandAddress">Address to send the command to</param>
    /// <param name="request">The request message</param>
    /// <param name="cancellationToken">An optional cancellation token</param>
    /// <param name="timeout">An optional timeout for the request</param>
    /// <typeparam name="TRequest">Type of request message</typeparam>
    /// <typeparam name="TResponse">Type of response message</typeparam>
    /// <returns>The response message</returns>
    Task<TResponse> GetCommandResponseAsync<TRequest, TResponse>(string commandAddress, TRequest request,
        CancellationToken cancellationToken = default, TimeSpan? timeout = default)
        where TRequest : class where TResponse : class;
}