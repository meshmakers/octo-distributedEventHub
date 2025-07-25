using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;

namespace SampleCli.Configuration;

internal class ConfigureDistributionEventHubOptions(IOptions<OctoMonitoringOptions> octoMonitoringOptions)
    : IConfigureNamedOptions<DistributionEventHubOptions>
{
    public void Configure(DistributionEventHubOptions options)
    {
        Configure(Microsoft.Extensions.Options.Options.DefaultName, options);
    }

    public void Configure(string? name, DistributionEventHubOptions options)
    {
        options.BrokerHost = octoMonitoringOptions.Value.MessageBrokerHost;
        options.BrokerPort = octoMonitoringOptions.Value.MessageBrokerPort;
        options.BrokerUser = octoMonitoringOptions.Value.MessageBrokerUser;
        options.BrokerPassword = octoMonitoringOptions.Value.MessageBrokerPassword;
        options.RepositoryHost = octoMonitoringOptions.Value.RepositoryHost;
        options.RepositoryUser = octoMonitoringOptions.Value.RepositoryUser;
        options.RepositoryPassword = octoMonitoringOptions.Value.RepositoryPassword;
        options.RepositoryUseTls = octoMonitoringOptions.Value.RepositoryUseTls;
        options.RepositoryAllowInsecureTls = octoMonitoringOptions.Value.RepositoryAllowInsecureTls;
    }
}