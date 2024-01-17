using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;
using SampleEvents.Messages;

namespace SampleCli.Commands;

public class PublishTenantReloadCommand : Command<OctoMonitoringOptions>
{
    private readonly IDistributionEventHubService _bus;

    public PublishTenantReloadCommand(ILogger<PublishTenantReloadCommand> logger,
        IOptions<OctoMonitoringOptions> options, IDistributionEventHubService bus)
        : base(logger, "publishTenantReload", "test", options)
    {
        _bus = bus;
    }

    public override async Task Execute()
    {
        Logger.LogInformation("Publish tenant reload command executing");

        var str = "testtenant";
        Logger.LogInformation("Publishing message {Value}", str);
        await _bus.PublishAsync(new ReloadTenant { TenantId = str });
        Logger.LogInformation("See you next time!");
    }
}