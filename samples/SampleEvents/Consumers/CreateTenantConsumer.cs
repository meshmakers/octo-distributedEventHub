using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class CreateTenantConsumer :
    IDistributedConsumer<CreateTenant>
{
    readonly ILogger<CreateTenantConsumer> _logger;

    public CreateTenantConsumer(ILogger<CreateTenantConsumer> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeAsync(IDistributedContext<CreateTenant> context)
    {
        _logger.LogInformation("Create Tenant received: {Text}", context.Message.TenantId);

        await context.PublishAsync(new BroadcastTest { TenantId = context.Message.TenantId });
    }
}