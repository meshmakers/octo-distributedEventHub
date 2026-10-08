namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Consumption options for a routed event consumer registered at runtime through
///     <see cref="IEventHubControl.RegisterRoutedEventConsumer{TMessage}(string, Func{TMessage, RoutedEventDeliveryContext, Task}, RoutedEventConsumerOptions)" />.
/// </summary>
/// <remarks>
///     None of these options is a RabbitMQ queue argument: prefetch is channel QoS, the concurrency limit
///     is enforced by MassTransit in-process, and coalescing is decided per delivery. They can therefore be
///     applied to an already existing durable queue without redeclaring it (no <c>PRECONDITION_FAILED</c>)
///     and without a queue migration.
/// </remarks>
public sealed record RoutedEventConsumerOptions
{
    /// <summary>
    ///     The legacy behaviour: MassTransit's default prefetch and concurrency, every message is handled.
    /// </summary>
    public static RoutedEventConsumerOptions Default { get; } = new();

    /// <summary>
    ///     One message at a time, and of the messages that piled up while nobody consumed only the newest
    ///     one is handled (AB#5709). Meant for periodic ticks such as cron pipeline triggers, where a
    ///     backlog of N ticks carries no more information than the last one.
    /// </summary>
    public static RoutedEventConsumerOptions LatestOnly { get; } = new()
    {
        PrefetchCount = 1,
        ConcurrentMessageLimit = 1,
        CoalescePendingMessages = true
    };

    /// <summary>
    ///     Number of unacknowledged messages the broker pushes to this consumer. <c>null</c> keeps the
    ///     MassTransit default (processor count × 2).
    /// </summary>
    public int? PrefetchCount { get; init; }

    /// <summary>
    ///     Number of messages handled concurrently. <c>null</c> keeps the MassTransit default (equal to the
    ///     prefetch count).
    /// </summary>
    public int? ConcurrentMessageLimit { get; init; }

    /// <summary>
    ///     When <c>true</c>, a delivered message is acknowledged without invoking the handler as long as newer
    ///     messages are already waiting in the queue — only the newest of a backlog reaches the handler.
    ///     Requires <see cref="PrefetchCount" /> and <see cref="ConcurrentMessageLimit" /> of <c>1</c> (or
    ///     unset, which then means <c>1</c>): with a larger prefetch the waiting messages would already be
    ///     in flight to this consumer and invisible to the queue's message count.
    /// </summary>
    public bool CoalescePendingMessages { get; init; }

    /// <summary>
    ///     The prefetch count that is actually applied, <c>null</c> for the transport default.
    /// </summary>
    internal int? EffectivePrefetchCount => CoalescePendingMessages ? PrefetchCount ?? 1 : PrefetchCount;

    /// <summary>
    ///     The concurrent message limit that is actually applied, <c>null</c> for the transport default.
    /// </summary>
    internal int? EffectiveConcurrentMessageLimit =>
        CoalescePendingMessages ? ConcurrentMessageLimit ?? 1 : ConcurrentMessageLimit;

    /// <summary>
    ///     Throws when the combination of options cannot work.
    /// </summary>
    /// <exception cref="ArgumentException">An option is out of range or contradicts coalescing.</exception>
    internal void Validate()
    {
        if (PrefetchCount is < 1)
        {
            throw new ArgumentException($"{nameof(PrefetchCount)} must be at least 1.", nameof(PrefetchCount));
        }

        if (ConcurrentMessageLimit is < 1)
        {
            throw new ArgumentException($"{nameof(ConcurrentMessageLimit)} must be at least 1.",
                nameof(ConcurrentMessageLimit));
        }

        if (CoalescePendingMessages && (EffectivePrefetchCount != 1 || EffectiveConcurrentMessageLimit != 1))
        {
            throw new ArgumentException(
                $"{nameof(CoalescePendingMessages)} requires {nameof(PrefetchCount)} = 1 and " +
                $"{nameof(ConcurrentMessageLimit)} = 1: messages already prefetched to the consumer are not " +
                "counted as waiting by the broker, so a larger window would let several of them through.",
                nameof(CoalescePendingMessages));
        }
    }
}
