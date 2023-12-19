namespace SampleEvents.Messages;

/// <summary>
/// Accepts an order
/// </summary>
public record OrderAccepted
{
    /// <summary>
    /// Returns the CorrelationId for the message
    /// </summary>
    public Guid CorrelationId { get; init; }
    
    /// <summary>
    /// Timestamp the order was accepted
    /// </summary>
    public DateTime Timestamp { get; init; }
}