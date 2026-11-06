namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Options for a dynamically registered command consumer
///     (<see cref="IEventHubControl.RegisterCommandConsumer{TMessage}(string, ExecuteCommandHandler{TMessage}, CommandConsumerOptions)" />).
/// </summary>
/// <remarks>
///     The endpoint is a shared (competing-consumer) endpoint: durable, auto-delete, non-exclusive. Multiple processes
///     registering the same command name become competing consumers on one queue. Senders use the send exchange
///     <c>{prefix}-{commandName}::send</c> (see <see cref="CommandSendOptions" />). For a command name that is
///     already served by the classic exclusive consumer, set <see cref="MigratesLegacyCommand" />. A consumer that must
///     stay exclusive (exactly one instance owns the address) declares it with <see cref="ExclusiveByDesign" /> /
///     <see cref="ExclusiveByDesignReason" />: it keeps the classic exclusive topology and logs no deprecation warning.
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

    /// <summary>
    ///     Declares the consumer <b>exclusive by design</b>: exactly one instance at a time owns the command address (for
    ///     example a command addressed to one pipeline instance), so it must not move to the shared topology. The value
    ///     is the reason, written for the next developer and for reviews (e.g. "addressed to one pipeline instance").
    ///     When set, the registration keeps the classic exclusive topology of the respective method unchanged
    ///     (non-durable, auto-delete, exclusive queue <c>{prefix}-{commandName}</c>, no <c>::send</c> exchange, no
    ///     bridge, senders unchanged) and the AB#6909 deprecation warning is not logged for it. <c>null</c> (default):
    ///     not declared — the options select the shared topology.
    /// </summary>
    /// <remarks>
    ///     Validated at registration (<see cref="ArgumentException" />): the reason must not be empty or whitespace,
    ///     and it cannot be combined with <see cref="MigratesLegacyCommand" />, <see cref="LegacyProbeInterval" /> or
    ///     <see cref="ConfigureConsumeTopology" /> (they belong to the shared topology). <see cref="PrefetchCount" />
    ///     and <see cref="ConcurrentMessageLimit" /> apply; <see cref="SuppressFaults" /> applies to the dynamic
    ///     <c>IEventHubControl.RegisterCommandConsumer</c> only. Prefer the factory <see cref="ExclusiveByDesign" />.
    /// </remarks>
    public string? ExclusiveByDesignReason { get; init; }

    /// <summary>
    ///     Options that declare a command consumer exclusive by design with the given reason
    ///     (see <see cref="ExclusiveByDesignReason" />).
    /// </summary>
    /// <param name="reason">Why exactly one instance must own the address; must not be empty or whitespace.</param>
    /// <returns>Options with <see cref="ExclusiveByDesignReason" /> set.</returns>
    /// <exception cref="ArgumentException"><paramref name="reason" /> is null, empty or whitespace.</exception>
    public static CommandConsumerOptions ExclusiveByDesign(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new CommandConsumerOptions { ExclusiveByDesignReason = reason };
    }

    /// <summary>
    ///     <c>true</c> when <see cref="ExclusiveByDesignReason" /> is set (validated by
    ///     <see cref="ValidateExclusiveByDesign" />).
    /// </summary>
    internal bool IsExclusiveByDesign => ExclusiveByDesignReason is not null;

    /// <summary>
    ///     Validates an exclusive-by-design declaration; does nothing when none is set.
    /// </summary>
    /// <exception cref="ArgumentException">Empty reason or a combination with a shared-topology option.</exception>
    internal void ValidateExclusiveByDesign(string paramName)
    {
        if (ExclusiveByDesignReason is null)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(ExclusiveByDesignReason))
        {
            throw new ArgumentException(
                "CommandConsumerOptions.ExclusiveByDesignReason must state why exactly one instance must own the " +
                "command address; an empty reason is not accepted.", paramName);
        }

        if (MigratesLegacyCommand)
        {
            throw new ArgumentException(
                "CommandConsumerOptions.ExclusiveByDesignReason and MigratesLegacyCommand contradict each other: " +
                "exclusive by design keeps the classic exclusive queue, MigratesLegacyCommand moves the consumer to " +
                "the shared queue. Set only one of them.", paramName);
        }

        if (LegacyProbeInterval is not null)
        {
            throw new ArgumentException(
                "CommandConsumerOptions.LegacyProbeInterval applies only with MigratesLegacyCommand and cannot be " +
                "combined with ExclusiveByDesignReason.", paramName);
        }

        if (ConfigureConsumeTopology)
        {
            throw new ArgumentException(
                "CommandConsumerOptions.ConfigureConsumeTopology cannot be combined with ExclusiveByDesignReason: an " +
                "exclusive-by-design consumer keeps the classic topology unchanged.", paramName);
        }
    }
}
