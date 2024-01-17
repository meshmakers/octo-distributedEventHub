namespace SampleEvents.Messages;

/// <summary>
///     Submits an order
/// </summary>
public record SubmitOrder
{
    /// <summary>
    ///     Returns the CorrelationId for the message
    /// </summary>
    public Guid CorrelationId { get; init; }

    /// <summary>
    ///     Timestamp of submitting the order
    /// </summary>
    public DateTime Timestamp { get; init; }
}