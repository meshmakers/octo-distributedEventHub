namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Options for a dynamically registered command consumer
///     (<see cref="IEventHubControl.RegisterCommandConsumer{TMessage}(string, ExecuteCommandHandler{TMessage}, CommandConsumerOptions)" />).
/// </summary>
/// <remarks>
///     The endpoint is always non-durable and auto-delete, so it matches the exchange the request client declares
///     (<c>exchange:{prefix}-{commandName}?temporary=true</c>). Multiple processes registering the same command name
///     become competing consumers on one queue.
/// </remarks>
public sealed record CommandConsumerOptions
{
    /// <summary>
    ///     When <c>true</c>, MassTransit binds the message-type exchange of <c>TMessage</c> to the command queue.
    ///     Must be <c>false</c> for CK method queues, otherwise a publish of the message type fans out to every
    ///     queue that consumes it. Default: <c>false</c>.
    /// </summary>
    public bool ConfigureConsumeTopology { get; init; }

    /// <summary>
    ///     Optional RabbitMQ prefetch count of the endpoint. <c>null</c> keeps the transport default.
    /// </summary>
    public int? PrefetchCount { get; init; }

    /// <summary>
    ///     Optional limit of concurrently processed messages on the endpoint. <c>null</c> keeps the transport default.
    /// </summary>
    public int? ConcurrentMessageLimit { get; init; }

    /// <summary>
    ///     For endpoints that carry sensitive payloads (CK methods: passwords, bearer tokens). When <c>true</c>:
    ///     an exception escaping the handler is logged (without the message) and swallowed — no bus retry, no
    ///     <c>Fault&lt;TMessage&gt;</c> (which embeds the full message) to the requester or as a publish; faulted
    ///     messages (e.g. deserialization failures) and skipped messages are discarded instead of being moved to the
    ///     <c>_error</c> / <c>_skipped</c> queues. The requester then runs into its timeout. Default: <c>false</c>.
    /// </summary>
    public bool SuppressFaults { get; init; }
}
