namespace SampleEvents.Messages;

public record UpdateEvent(string TenantId, string Name, DateTime DateTime, object Value)
{
}