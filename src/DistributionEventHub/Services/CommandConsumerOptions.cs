namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Options for a dynamically registered command consumer
///     (<see cref="IEventHubControl.RegisterCommandConsumer{TMessage}(string, ExecuteCommandHandler{TMessage}, CommandConsumerOptions)" />).
/// </summary>
/// <remarks>
///     The endpoint is a shared (competing-consumer) endpoint: durable, auto-delete, non-exclusive. Multiple processes
///     registering the same command name become competing consumers on one queue. Senders use the send exchange
///     <c>{prefix}-{commandName}::send</c> (see <see cref="CommandSendOptions" />). For a command name that is
///     already served by the classic exclusive consumer, set <see cref="MigratesLegacyCommand" />.
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

    /// <summary>
    ///     Set to <c>true</c> when the command name is, or was until now, served by the classic exclusive consumer
    ///     (<c>AddCommandConsumer(name)</c> / <c>RegisterCommandConsumer(name, handler)</c>) and its senders
    ///     (<c>AddCommandClient</c>, <c>GetCommandResponseAsync(..., timeout)</c>) have not been changed. The
    ///     consumer then uses the queue <c>{prefix}-{commandName}::shared</c> (a name no old replica uses, so old and
    ///     new replicas never hit PRECONDITION_FAILED or RESOURCE_LOCKED) and serves the classic sender address
    ///     <c>{prefix}-{commandName}</c> as soon as no classic consumer is alive: while an old replica runs, it keeps
    ///     all classic traffic and the new replicas stand by; when the last old replica is gone, the new replicas
    ///     take over — without a flag day, without loss and without double processing. Default: <c>false</c>
    ///     (CK method endpoints, new command names).
    /// </summary>
    /// <remarks>
    ///     Migration guide and rollout order: <c>docs/DEVELOPER.md</c> → "Migrating exclusive command queues".
    ///     The handler must be safe under competing consumers (no state shared between messages, no
    ///     "only one instance" assumption).
    /// </remarks>
    public bool MigratesLegacyCommand { get; init; }

    /// <summary>
    ///     Interval of the check whether a classic exclusive consumer is alive (only with
    ///     <see cref="MigratesLegacyCommand" />). Bounds the take-over delay after the last old replica stopped.
    ///     <c>null</c> uses 2 seconds.
    /// </summary>
    public TimeSpan? LegacyProbeInterval { get; init; }
}
