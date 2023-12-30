namespace SampleEvents.Messages;

public record BroadcastTest
{
    public string TenantId { get; init; } = null!;
}