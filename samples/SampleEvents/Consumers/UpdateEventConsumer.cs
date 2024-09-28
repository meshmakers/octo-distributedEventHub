using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class UpdateEventConsumer(ILogger<UpdateEventConsumer> logger) : IDistributedConsumer<UpdateEvent>
{
    public Task ConsumeAsync(IDistributedContext<UpdateEvent> context)
    {
        logger.LogInformation("Update event received: {Name} {DateTime} {Value}", context.Message.Name,
            context.Message.DateTime, context.Message.Value);

        return Task.CompletedTask;
    }
}