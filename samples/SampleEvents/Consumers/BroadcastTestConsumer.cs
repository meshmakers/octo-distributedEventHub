using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class BroadcastTestConsumer :
    IDistributedConsumer<BroadcastTest>
{
    private readonly ILogger<BroadcastTestConsumer> _logger;

    public BroadcastTestConsumer(ILogger<BroadcastTestConsumer> logger)
    {
        _logger = logger;
    }

    public Task ConsumeAsync(IDistributedContext<BroadcastTest> context)
    {
        _logger.LogInformation("broadcast test received: {Text}", context.Message.TenantId);

        return Task.CompletedTask;
    }
}