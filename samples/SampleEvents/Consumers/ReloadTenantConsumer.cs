using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Microsoft.Extensions.Logging;
using SampleEvents.Messages;

namespace SampleEvents.Consumers;

public class ReloadTenantConsumer :
    IDistributedConsumer<ReloadTenant>
{
    readonly ILogger<ReloadTenantConsumer> _logger;

    public ReloadTenantConsumer(ILogger<ReloadTenantConsumer> logger)
    {
        _logger = logger;
    }

    public Task ConsumeAsync(IDistributedContext<ReloadTenant> context)
    {
        _logger.LogInformation("Reload Tenant received: {Text}", context.Message.TenantId);

        return Task.CompletedTask;
    }
}
