namespace SampleEvents.Messages;

public record ReserveStockResponse
{
    /// <summary>
    ///     Returns the CorrelationId for the message
    /// </summary>
    public Guid CorrelationId { get; init; }

    /// <summary>
    ///     Returns a value
    /// </summary>
    public string Value { get; init; } = null!;
}