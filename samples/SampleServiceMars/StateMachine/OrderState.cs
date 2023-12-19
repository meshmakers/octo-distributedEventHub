using MassTransit;

namespace SampleServiceMars.StateMachine;

internal class OrderState : SagaStateMachineInstance, ISagaVersion
{
    /// <inheritdoc />
    public Guid CorrelationId { get; set; }

    /// <summary>
    /// the current saga state
    /// </summary>
    public string CurrentState { get; set; } = null!;

    public DateTime? SubmittedDateTime { get; set; }
    public string? StockInfo { get; set; }
    public int Version { get; set; }
}