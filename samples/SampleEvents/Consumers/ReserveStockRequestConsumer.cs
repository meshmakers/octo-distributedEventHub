using Meshmakers.Octo.Common.DistributionEventHub.Commands;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class ReserveStockRequestConsumer :
    IDistributedConsumer<ReserveStockRequest>
{
    readonly ILogger<ReserveStockRequestConsumer> _logger;

    public ReserveStockRequestConsumer(ILogger<ReserveStockRequestConsumer> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeAsync(IDistributedContext<ReserveStockRequest> context)
    {
        _logger.LogInformation("Stock request received: {Text}", context.Message.CorrelationId);
        
        await Task.Delay(1000);

        await context.RespondAsync(new ReserveStockResponse() { Value = DateTime.UtcNow + "_Response" });
    }
}