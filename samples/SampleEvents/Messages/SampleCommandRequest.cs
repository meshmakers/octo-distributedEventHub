namespace SampleEvents.Messages;

public record SampleCommandRequest()
{
    public string Value { get; init; } = null!;
}