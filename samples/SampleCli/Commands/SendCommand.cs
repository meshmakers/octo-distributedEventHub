using System.Globalization;
using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Octo.Common.DistributionEventHub.Commands;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;
using SampleEvents.Consumers;
using SampleEvents.Messages;

namespace SampleCli.Commands;

public class SendCommand : Command<OctoMonitoringOptions>
{
    private readonly ICommandClient<SampleCommandRequest> _commandClient;

    public SendCommand(ILogger<SendCommand> logger,
        IOptions<OctoMonitoringOptions> options, ICommandClient<SampleCommandRequest> commandClient)
        : base(logger, "send", "Sends a command", options)
    {
        _commandClient = commandClient;
    }

    public override async Task Execute()
    {
        Logger.LogInformation("Send command executing");

        var str = DateTime.Now.ToString(CultureInfo.InvariantCulture);
        Logger.LogInformation("Sending value {Value}", str);

        var response = await _commandClient.GetResponse<SampleCommandResponse>(new SampleCommandRequest { Value = str });
        Logger.LogInformation("Response received: {Value}", response.Value);

        Logger.LogInformation("See you next time!");
    }
}