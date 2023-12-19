using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;
using SampleEvents.Messages;

namespace SampleCli.Commands;

public class StartSagaCommand : Command<OctoMonitoringOptions>
{
    private readonly IDistributionEventHubService _bus;

    public StartSagaCommand(ILogger<StartSagaCommand> logger,
        IOptions<OctoMonitoringOptions> options, IDistributionEventHubService bus)
        : base(logger, "startSaga", "start saga command", options)
    {
        _bus = bus;
    }

    public override async Task Execute()
    {
        Logger.LogInformation("start saga command executing");

        await _bus.PublishAsync(new SubmitOrder { CorrelationId = Guid.NewGuid(), Timestamp = DateTime.UtcNow });

        Logger.LogInformation("See you next time!");
    }
}