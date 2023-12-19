namespace SampleEvents.Messages;

public record ReserveStockRequest()
{
    /// <summary>
    /// Returns the CorrelationId for the message
    /// </summary>
    public Guid CorrelationId { get; init; }
}