using Meshmakers.Common.CommandLineParser;
using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Common.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;

namespace SampleCli.Commands;

internal class ConfigOctoCommand : Command<OctoMonitoringOptions>
{
    private readonly IConfigWriter _configWriter;
    private readonly IArgument _messageBrokerHost;
    private readonly IArgument _messageBrokerUser;
    private readonly IArgument _messageBrokerPassword;
    private readonly IArgument _repositoryHost;
    private readonly IArgument _repositoryUser;
    private readonly IArgument _repositoryPassword;
    private readonly IArgument _repositoryUseTls;
    private readonly IArgument _repositoryAllowInsecureTls;

    public ConfigOctoCommand(ILogger<ConfigOctoCommand> logger, IOptions<OctoMonitoringOptions> options,
        IConfigWriter configWriter)
        : base(logger, "Config", "Configures the tool.", options)
    {
        _configWriter = configWriter;

        _messageBrokerHost = CommandArgumentValue.AddArgument("mbh", "messageBrokerHost",
            new[] { "Hostname of message broker (e. g. 'localhost')" }, true, 1);
        _messageBrokerUser = CommandArgumentValue.AddArgument("mbu", "messageBrokerUser",
            new[] { "BrokerUser of message broker (e. g. 'guest')" }, true, 1);
        _messageBrokerPassword = CommandArgumentValue.AddArgument("mbp", "messageBrokerPassword",
            new[] { "BrokerPassword of message broker (e. g. 'guest')" }, true, 1);

        _repositoryHost = CommandArgumentValue.AddArgument("hr", "repositoryHost",
            new[] { "Hostname of persistence repository (e. g. 'localhost:27017')" }, true, 1);
        _repositoryUser = CommandArgumentValue.AddArgument("ru", "repositoryUser",
            new[] { "BrokerUser of persistence repository (e. g. 'guest')" }, true, 1);
        _repositoryPassword = CommandArgumentValue.AddArgument("rp", "repositoryPassword",
            new[] { "BrokerPassword of persistence repository (e. g. 'guest')" }, true, 1);
        _repositoryUseTls = CommandArgumentValue.AddArgument("rTls", "repositoryUseTls",
            new[] { "Use TLS for repository connection" }, false, 1);
        _repositoryAllowInsecureTls = CommandArgumentValue.AddArgument("rInsecureTls", "repositoryAllowInsecureTls",
            new[] { "Use TLS for repository connection" }, false, 1);
    }

    public override Task Execute()
    {
        Logger.LogInformation("Configuring the tool");


        Options.Value.MessageBrokerHost =
            CommandArgumentValue.GetArgumentScalarValue<string>(_messageBrokerHost).ToLower();

        Options.Value.MessageBrokerUser =
            CommandArgumentValue.GetArgumentScalarValue<string>(_messageBrokerUser).ToLower();

        Options.Value.MessageBrokerPassword =
            CommandArgumentValue.GetArgumentScalarValue<string>(_messageBrokerPassword).ToLower();
        
        Options.Value.RepositoryHost =
            CommandArgumentValue.GetArgumentScalarValue<string>(_repositoryHost).ToLower();

        Options.Value.RepositoryUser =
            CommandArgumentValue.GetArgumentScalarValue<string>(_repositoryUser).ToLower();

        Options.Value.RepositoryPassword =
            CommandArgumentValue.GetArgumentScalarValue<string>(_repositoryPassword).ToLower();

        if (CommandArgumentValue.IsArgumentUsed(_repositoryUseTls))
        {
            Options.Value.RepositoryUseTls = CommandArgumentValue.GetArgumentScalarValue<bool>(_repositoryUseTls);
        }
        
        if (CommandArgumentValue.IsArgumentUsed(_repositoryAllowInsecureTls))
        {
            Options.Value.RepositoryAllowInsecureTls = CommandArgumentValue.GetArgumentScalarValue<bool>(_repositoryAllowInsecureTls);
        }

        _configWriter.WriteSettings(Constants.OctoDistributionEventMonitorUserFolderName);

        return Task.CompletedTask;
    }
}