using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;

namespace SampleCli.Configuration;

internal class ConfigureDistributionEventHubOptions : IConfigureNamedOptions<DistributionEventHubOptions>
{
    private readonly IOptions<OctoMonitoringOptions> _octoMonitoringOptions;

    public ConfigureDistributionEventHubOptions(IOptions<OctoMonitoringOptions> octoMonitoringOptions)
    {
        _octoMonitoringOptions = octoMonitoringOptions;
    }

    public void Configure(DistributionEventHubOptions options)
    {
        Configure(Microsoft.Extensions.Options.Options.DefaultName, options);
    }

    public void Configure(string? name, DistributionEventHubOptions options)
    {
        options.BrokerHost = _octoMonitoringOptions.Value.MessageBrokerHost;
        options.BrokerPort = _octoMonitoringOptions.Value.MessageBrokerPort;
        options.BrokerUser = _octoMonitoringOptions.Value.MessageBrokerUser;
        options.BrokerPassword = _octoMonitoringOptions.Value.MessageBrokerPassword;
        options.RepositoryHost = _octoMonitoringOptions.Value.RepositoryHost;
        options.RepositoryUser = _octoMonitoringOptions.Value.RepositoryUser;
        options.RepositoryPassword = _octoMonitoringOptions.Value.RepositoryPassword;
        options.RepositoryUseTls = _octoMonitoringOptions.Value.RepositoryUseTls;
        options.RepositoryAllowInsecureTls = _octoMonitoringOptions.Value.RepositoryAllowInsecureTls;
    }
}