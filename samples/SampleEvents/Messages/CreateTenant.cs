namespace SampleEvents.Messages;

public record CreateTenant
{
    public string TenantId { get; init; } = null!;
}