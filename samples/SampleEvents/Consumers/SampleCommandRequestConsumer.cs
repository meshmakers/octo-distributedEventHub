using Meshmakers.Octo.Common.DistributionEventHub.Commands;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class SampleCommandRequestConsumer :
    IDistributedConsumer<SampleCommandRequest>
{
    readonly ILogger<SampleCommandRequestConsumer> _logger;

    public SampleCommandRequestConsumer(ILogger<SampleCommandRequestConsumer> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeAsync(IDistributedContext<SampleCommandRequest> context)
    {
        _logger.LogInformation("Command request received: {Text}", context.Message.Value);
        
        await Task.Delay(1000);

        await context.RespondAsync(new SampleCommandResponse { Value = context.Message.Value + "_Response" });
    }
}