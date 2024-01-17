namespace SampleEvents.Messages;

public record SampleCommandResponse
{
    public string Value { get; init; } = null!;
}