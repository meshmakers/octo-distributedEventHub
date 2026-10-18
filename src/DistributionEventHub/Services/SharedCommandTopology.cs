using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Broker topology of a shared (competing-consumer) command endpoint, used by the dynamic
///     <c>RegisterCommandConsumer(..., CommandConsumerOptions)</c> and the DI
///     <c>AddCommandConsumer(name, CommandConsumerOptions)</c>.
/// </summary>
internal static class SharedCommandTopology
{
    /// <summary>
    ///     Shared (competing-consumer) command topology:
    ///     <list type="bullet">
    ///         <item>
    ///             Queue durable + auto-delete: MassTransit forces EXCLUSIVE on non-durable auto-delete queues
    ///             (second process → RESOURCE_LOCKED, endpoint never ready). A durable auto-delete queue is
    ///             non-exclusive and is still removed when its last consumer goes away (graceful stop or
    ///             broker-detected dead connection).
    ///         </item>
    ///         <item>
    ///             Senders publish to a separate, stable send exchange (durable, not auto-delete) bound to the
    ///             queue. The endpoint's own exchange is auto-deleted together with the queue; sending to a deleted
    ///             exchange closes the channel (404) and costs seconds of recovery, whereas a mandatory send to the
    ///             stable exchange without a bound queue is returned in milliseconds.
    ///         </item>
    ///     </list>
    /// </summary>
    internal static void Configure(IRabbitMqReceiveEndpointConfigurator rabbitConfigurator, string sendExchangeName,
        int? prefetchCount)
    {
        rabbitConfigurator.Durable = true;
        rabbitConfigurator.AutoDelete = true;
        rabbitConfigurator.Exclusive = false;
        if (prefetchCount is { } prefetch)
        {
            rabbitConfigurator.PrefetchCount = prefetch;
        }

        rabbitConfigurator.Bind(sendExchangeName, b =>
        {
            b.ExchangeType = "fanout";
            b.Durable = true;
            b.AutoDelete = false;
        });
    }
}
