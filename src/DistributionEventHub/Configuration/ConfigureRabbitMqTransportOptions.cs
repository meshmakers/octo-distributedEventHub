using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.Options;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration;

internal class ConfigureRabbitMqTransportOptions : IConfigureNamedOptions<RabbitMqTransportOptions>
{
    private readonly IOptions<DistributionEventHubOptions> _distributionEventHubOptions;

    public ConfigureRabbitMqTransportOptions(IOptions<DistributionEventHubOptions> distributionEventHubOptions)
    {
        _distributionEventHubOptions = distributionEventHubOptions;
    }

    public void Configure(RabbitMqTransportOptions options)
    {
        Configure(Microsoft.Extensions.Options.Options.DefaultName, options);
    }

    public void Configure(string? name, RabbitMqTransportOptions options)
    {
        options.Host = _distributionEventHubOptions.Value.BrokerHost;
        options.Port = _distributionEventHubOptions.Value.BrokerPort;
        options.User = _distributionEventHubOptions.Value.BrokerUser;
        options.Pass = _distributionEventHubOptions.Value.BrokerPassword;
    }
}