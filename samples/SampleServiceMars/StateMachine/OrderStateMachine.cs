using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using SampleEvents.Messages;

namespace SampleServiceMars.StateMachine;

internal class OrderStateMachine :
    MassTransitStateMachine<OrderState>
{
    // public State Final { get; private set; } 

    public OrderStateMachine(ILogger<OrderStateMachine> logger, IDistributionEventHubService eventHub)
    {
        InstanceState(x => x.CurrentState);

        Request(() => ReserveStock);

        Initially(
            // Behavior Starts
            When(SubmitOrder)
                .Then(behaviorContext =>
                {
                    var saga = behaviorContext.Saga;
                    var message = behaviorContext.Message;

                    logger.LogInformation("Order submitted received: {Text}", message.CorrelationId);

                    saga.SubmittedDateTime = message.Timestamp;
                })
                .Request(ReserveStock, sendContext =>
                {
                    var saga = sendContext.Saga;
                    var msg = sendContext.Message;
                    logger.LogInformation("Reserve stock published: {Text}", sendContext.Message.CorrelationId);

                    return new ReserveStockRequest { CorrelationId = sendContext.Message.CorrelationId };
                })
                .TransitionTo(Submitted)
            // Behavior completes and state persisted
        );

        During(Submitted,
            // handle the consumer successfully responding
            When(ReserveStock.Completed)
                .ThenAsync(async context =>
                {
                    context.Saga.StockInfo = context.Message.Value;

                    logger.LogInformation("Order accepted published: {Text}", context.Message.Value);
                    
                    // Use IDistributionEventHubService for proper instance prefix handling
                    await eventHub.PublishAsync(new OrderAccepted
                    {
                        CorrelationId = context.Saga.CorrelationId,
                        Timestamp = context.Saga.SubmittedDateTime!.Value
                    });
                })
                .TransitionTo(Accepted),

            // handle the consumer throwing an exception
            When(ReserveStock.Faulted)
                .TransitionTo(Failed)
        );
    }

    //  public State Initial { get; private set; } 
    public State Submitted { get; } = null!;

    public State Accepted { get; } = null!;
    public State Failed { get; } = null!;

    public Request<OrderState, ReserveStockRequest, ReserveStockResponse> ReserveStock { get; } = null!;

    public Event<SubmitOrder> SubmitOrder { get; } = null!;
    // public Event<OrderAccepted> OrderAccepted { get; private set; } = null!;
}