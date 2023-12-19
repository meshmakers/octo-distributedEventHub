namespace SampleEvents.Messages;

public record ReloadTenant
{
    public string TenantId { get; init; } = null!;
}