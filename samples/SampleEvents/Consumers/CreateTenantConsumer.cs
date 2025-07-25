using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class CreateTenantConsumer :
    IDistributedConsumer<CreateTenant>
{
    private readonly ILogger<CreateTenantConsumer> _logger;
    private readonly IDistributionEventHubService _eventHub;

    public CreateTenantConsumer(ILogger<CreateTenantConsumer> logger, IDistributionEventHubService eventHub)
    {
        _logger = logger;
        _eventHub = eventHub;
    }

    public async Task ConsumeAsync(IDistributedContext<CreateTenant> context)
    {
        _logger.LogInformation("Create Tenant received: {Text}", context.Message.TenantId);

        // Use IDistributionEventHubService for proper instance prefix handling
        await _eventHub.PublishAsync(new BroadcastTest { TenantId = context.Message.TenantId });
    }
}