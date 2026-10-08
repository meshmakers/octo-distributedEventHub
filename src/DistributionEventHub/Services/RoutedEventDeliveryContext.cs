namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Delivery information handed to a routed event handler together with the message.
/// </summary>
/// <param name="CoalescedMessageCount">
///     Number of older messages that were acknowledged without reaching the handler since the previous
///     handled message, because <see cref="RoutedEventConsumerOptions.CoalescePendingMessages" /> is on and a
///     newer message was already waiting. Always <c>0</c> when coalescing is off.
/// </param>
public sealed record RoutedEventDeliveryContext(long CoalescedMessageCount)
{
    /// <summary>
    ///     A delivery without coalescing information.
    /// </summary>
    public static RoutedEventDeliveryContext None { get; } = new(0);
}
