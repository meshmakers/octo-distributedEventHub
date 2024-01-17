using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class OrderAcceptedConsumer :
    IDistributedConsumer<OrderAccepted>
{
    private readonly ILogger<OrderAcceptedConsumer> _logger;

    public OrderAcceptedConsumer(ILogger<OrderAcceptedConsumer> logger)
    {
        _logger = logger;
    }

    public Task ConsumeAsync(IDistributedContext<OrderAccepted> context)
    {
        _logger.LogInformation("Order accepted received: {Text}", context.Message.CorrelationId);

        return Task.CompletedTask;

        // await context.Publish<OrderSubmitted>(new
        // {
        //     OrderId = context.Message.TenantId
        // });
    }
}