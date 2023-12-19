using MassTransit;
using SampleEvents.Messages;

namespace SampleServiceMars.StateMachine;

internal class OrderStateMachine :
    MassTransitStateMachine<OrderState>
{
    //  public State Initial { get; private set; } 
    public State Submitted { get; private set; } = null!;

    public State Accepted { get; private set; } = null!;
    public State Failed { get; private set; } = null!;
    // public State Final { get; private set; } 

    public OrderStateMachine(ILogger<OrderStateMachine> logger)
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
                .Request(ReserveStock, sendContext => {
                    var saga = sendContext.Saga;
                    var msg = sendContext.Message;
                    logger.LogInformation("Reserve stock published: {Text}", sendContext.Message.CorrelationId);
                
                    return new ReserveStockRequest {CorrelationId = sendContext.Message.CorrelationId};
                })
                .TransitionTo(Submitted)
            // Behavior completes and state persisted
        );
        
        During(Submitted,
            // handle the consumer successfully responding
            When(ReserveStock.Completed)
                .Publish(context =>
                {
                    context.Saga.StockInfo = context.Message.Value;
                    
                    logger.LogInformation("Order accepted published: {Text}", context.Message.Value);
                    return new OrderAccepted
                    {
                        CorrelationId = context.Saga.CorrelationId,
                        Timestamp = context.Saga.SubmittedDateTime!.Value
                    };
                })
                .TransitionTo(Accepted),

            // handle the consumer throwing an exception
            When(ReserveStock.Faulted)
                .TransitionTo(Failed)
        );
    }

    public Request<OrderState, ReserveStockRequest, ReserveStockResponse> ReserveStock { get; private set; } = null!;

    public Event<SubmitOrder> SubmitOrder { get; private set; } = null!;
   // public Event<OrderAccepted> OrderAccepted { get; private set; } = null!;
}