namespace SampleEvents.Messages;

public record UpdateEvent
{
    public required string TenantId { get; init; }
    public required string Name { get; init; }
    public required DateTime DateTime { get; init; }
    public required object Value { get; init; }
}