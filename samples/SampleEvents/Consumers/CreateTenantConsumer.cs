using Meshmakers.Octo.Common.DistributionEventHub.Commands;
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

    public Task ConsumeAsync(IDistributedContext<CreateTenant> context)
    {
        _logger.LogInformation("Create Tenant received: {Text}", context.Message.TenantId);

        return Task.CompletedTask;

        // await context.Publish<OrderSubmitted>(new
        // {
        //     OrderId = context.Message.TenantId
        // });
    }
}