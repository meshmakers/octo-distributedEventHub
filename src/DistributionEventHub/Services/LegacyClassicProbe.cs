using System.Text;
using RabbitMQ.Client;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>How the <see cref="LegacyCommandBridge" /> checks whether the classic queue of a migrated command exists.</summary>
internal enum LegacyClassicProbeKind
{
    /// <summary>
    ///     Passive <c>queue.declare</c> of the classic queue. Tells "exclusive queue of another connection" (405) from "does
    ///     not exist" (404), but both answers close the channel and RabbitMQ logs each as an error.
    /// </summary>
    PassiveDeclare,

    /// <summary>
    ///     A <c>mandatory</c> publish of a probe message to the default exchange with the classic queue name as routing key,
    ///     on a channel with publisher confirms. No queue → the broker returns the message (<c>basic.return</c>, NO_ROUTE),
    ///     which is normal protocol and not logged as an error. Queue present → the message is routed into it.
    /// </summary>
    MandatoryPublish
}

/// <summary>
///     Chooses the classic probe of the <see cref="LegacyCommandBridge" /> and builds the probe message (AB#8408).
/// </summary>
/// <remarks>
///     <para>
///         <b>Why two probes.</b> After the migration the classic queue does not exist, and every passive declare of a
///         missing queue is two ERROR lines in RabbitMQ's log ("channel exception not_found" + "Channel error on
///         connection"). The back-off of AB#8404 bounded them per bridge, but one bridge runs per migrated command and
///         replica, so the rate still grew with the replica count (test-2 with two replicas: 0.233 ERROR lines/s, above the
///         0.2/s alarm). A mandatory publish to the default exchange answers the same question — does a queue with this
///         name exist? — without any channel exception.
///     </para>
///     <para>
///         <b>When which probe.</b> The mandatory publish is used only while the bridge is
///         <see cref="LegacyCommandBridgeState.Bound" />, i.e. when the previous probe found no classic queue. Then a routed
///         probe means a classic consumer appeared while bound (rollback) — exactly the case the probe is there for, and
///         the bridge steps aside at once. Every other state (start, a classic consumer alive, shared queue missing,
///         faulted) uses the passive declare, so a live classic consumer — during a migration rollout — never receives probe
///         messages; those states are transient, and the first pass of a bridge costs at most one error pair.
///     </para>
///     <para>
///         <b>A routed probe</b> reaches the classic consumer only in that rollback case, once per detection. It carries a
///         MassTransit envelope with a message type no consumer handles and expires at once if it cannot be delivered
///         (<c>expiration: 0</c>), so a classic MassTransit endpoint skips it (moves it to its <c>_skipped</c> queue) instead
///         of faulting.
///     </para>
///     <para>
///         <b>Fallback.</b> Publishing to the default exchange needs write permission on <c>amq.default</c>. The restricted
///         adapter user (AB#7338) does not have it; when the broker refuses the publish (403 ACCESS_REFUSED) the bridge
///         falls back to the passive declare with the back-off of <see cref="LegacyProbeSchedule" /> for its lifetime.
///     </para>
/// </remarks>
internal static class LegacyClassicProbe
{
    /// <summary>Message type of the probe; no consumer handles it, so a MassTransit endpoint skips it.</summary>
    internal const string MessageTypeUrn = "urn:message:Meshmakers.Octo.Common.DistributionEventHub:LegacyCommandBridgeProbe";

    /// <summary>Content type of the probe (MassTransit's JSON envelope).</summary>
    internal const string ContentType = "application/vnd.masstransit+json";

    /// <summary>Selects the probe for the next reconcile pass.</summary>
    /// <param name="current">State after the previous pass</param>
    /// <param name="publishProbeUnavailable">The broker refused the publish probe before (no write on <c>amq.default</c>)</param>
    public static LegacyClassicProbeKind Select(LegacyCommandBridgeState current, bool publishProbeUnavailable) =>
        current == LegacyCommandBridgeState.Bound && !publishProbeUnavailable
            ? LegacyClassicProbeKind.MandatoryPublish
            : LegacyClassicProbeKind.PassiveDeclare;

    /// <summary>Properties of the probe message: transient, expires at once unless it can be delivered right away.</summary>
    public static BasicProperties CreateProperties(Guid messageId) => new()
    {
        ContentType = ContentType,
        DeliveryMode = DeliveryModes.Transient,
        Expiration = "0",
        MessageId = messageId.ToString("D"),
        Type = MessageTypeUrn
    };

    /// <summary>Body of the probe message: a minimal MassTransit envelope of the unhandled probe type.</summary>
    public static byte[] CreateBody(Guid messageId) => Encoding.UTF8.GetBytes(
        $"{{\"messageId\":\"{messageId:D}\",\"messageType\":[\"{MessageTypeUrn}\"],\"message\":{{}}}}");
}
