namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Decides per delivery whether a message is superseded by newer messages that are already waiting in
///     the queue (AB#5709). One instance per receive endpoint.
/// </summary>
/// <remarks>
///     <para>
///         Works together with a prefetch count and concurrency limit of 1: the delivery being decided on is
///         the only unacknowledged message, so the queue's ready-message count is exactly the number of newer
///         messages behind it. A backlog of N messages therefore produces N − 1 skips followed by one handled
///         message, and a message that arrives while the handler runs is handled afterwards (it is the newest
///         one) — the semantics of <c>x-max-length = 1</c> with <c>x-overflow = drop-head</c>, but without a
///         queue argument that would have to be redeclared.
///     </para>
///     <para>
///         When the pending count cannot be determined (no RabbitMQ channel, broker error) the message is
///         handled: coalescing fails open to the previous behaviour instead of losing an execution.
///     </para>
/// </remarks>
internal sealed class PendingMessageCoalescer
{
    private long _coalesced;

    /// <summary>
    ///     Number of messages skipped since the last handled one.
    /// </summary>
    public long PendingCoalescedCount => Interlocked.Read(ref _coalesced);

    /// <summary>
    ///     Decides about one delivery.
    /// </summary>
    /// <param name="pendingMessageCount">
    ///     Provider of the number of messages waiting in the queue behind the current delivery, or
    ///     <c>null</c> when unknown.
    /// </param>
    /// <returns>
    ///     <c>null</c> when the delivery is superseded and must not reach the handler; otherwise the delivery
    ///     context carrying the number of messages coalesced into this one.
    /// </returns>
    public async Task<RoutedEventDeliveryContext?> DecideAsync(Func<Task<uint?>> pendingMessageCount)
    {
        uint? pending;
        try
        {
            pending = await pendingMessageCount().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Fail open: an unknown backlog must not swallow the execution.
            pending = null;
        }

        if (pending is > 0)
        {
            Interlocked.Increment(ref _coalesced);
            return null;
        }

        return new RoutedEventDeliveryContext(Interlocked.Exchange(ref _coalesced, 0));
    }
}
