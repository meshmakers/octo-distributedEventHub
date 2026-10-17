namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Options for the shared-command-endpoint send overload <see cref="IDistributionEventHubService.GetCommandResponseAsync{TRequest,TResponse}(string, TRequest, CommandSendOptions, CancellationToken)" />.
/// </summary>
public sealed record CommandSendOptions
{
    /// <summary>
    ///     Response timeout. <c>null</c> uses the MassTransit default request timeout.
    /// </summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>
    ///     Sets the RabbitMQ <c>mandatory</c> flag. If no queue is bound to the command exchange, the broker returns
    ///     the message and the call fails fast with <see cref="CommandNotRoutableException" /> instead of waiting for
    ///     the timeout.
    /// </summary>
    public bool Mandatory { get; init; }

    /// <summary>
    ///     Optional message time-to-live, so a stale request expires in the queue instead of being executed late.
    ///     Typically equal to <see cref="Timeout" />.
    /// </summary>
    public TimeSpan? TimeToLive { get; init; }

    /// <summary>
    ///     <c>false</c> sends the request as a non-persistent (transient) message, so the broker does not write it to
    ///     its persistent message store. Use it for sensitive payloads. Default: <c>true</c> (MassTransit default).
    /// </summary>
    /// <remarks>
    ///     Residual risk: RabbitMQ may still page transient messages to disk under memory pressure (classic queues
    ///     v2 keep message bodies on disk as part of normal paging). Combine with a short <see cref="TimeToLive" />.
    /// </remarks>
    public bool Persistent { get; init; } = true;
}
