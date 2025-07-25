using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.Options;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration;

internal class ConfigureRabbitMqTransportOptions(IOptions<DistributionEventHubOptions> distributionEventHubOptions)
    : IConfigureNamedOptions<RabbitMqTransportOptions>
{
    public void Configure(RabbitMqTransportOptions options)
    {
        Configure(Microsoft.Extensions.Options.Options.DefaultName, options);
    }

    public void Configure(string? name, RabbitMqTransportOptions options)
    {
        options.Host = distributionEventHubOptions.Value.BrokerHost;
        options.Port = distributionEventHubOptions.Value.BrokerPort;
        options.User = distributionEventHubOptions.Value.BrokerUser;
        options.Pass = distributionEventHubOptions.Value.BrokerPassword;
    }
}