using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;
using SampleEvents.Messages;

namespace SampleCli.Commands;

public class PublishRoutedEventsCommand : Command<OctoMonitoringOptions>
{
    private readonly IDistributionEventHubService _bus;

    public PublishRoutedEventsCommand(ILogger<PublishRoutedEventsCommand> logger,
        IOptions<OctoMonitoringOptions> options, IDistributionEventHubService bus)
        : base(logger, "publishRoutedEvents", "test", options)
    {
        _bus = bus;
    }

    public override async Task Execute()
    {
        long i = 0;
        var run = true;
        Logger.LogInformation("Publish command executing");
        Console.CancelKeyPress += (sender, args) =>
        {
            Logger.LogInformation("CTRL-C pressed, exiting");
            args.Cancel = true;
            run = false;
        };
        while (run)
        {
            Logger.LogInformation("Press CTRL-C to exit");

            var str = DateTime.Now;
            Logger.LogInformation("Publishing message {Value}", str);
            await _bus.PublishAsync(new UpdateEvent("test", "demo", str, i));
            await Task.Delay(1000);
            i++;
        }

        Logger.LogInformation("See you next time!");
    }
}