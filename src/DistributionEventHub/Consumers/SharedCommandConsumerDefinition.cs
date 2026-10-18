using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;

namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

/// <summary>
///     Endpoint definition of a DI-registered command consumer on the shared topology
///     (<c>AddCommandConsumer(name, CommandConsumerOptions)</c>, AB#6909).
/// </summary>
internal sealed class SharedCommandConsumerDefinition<TConsumer> : ConsumerDefinition<TConsumer>
    where TConsumer : class, IConsumer
{
    private readonly string _sendExchangeName;
    private readonly CommandConsumerOptions _options;

    public SharedCommandConsumerDefinition(string queueName, string sendExchangeName, CommandConsumerOptions options)
    {
        _sendExchangeName = sendExchangeName;
        _options = options;
        EndpointName = queueName;
        if (options.ConcurrentMessageLimit is { } concurrentMessageLimit)
        {
            ConcurrentMessageLimit = concurrentMessageLimit;
        }
    }

    protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
        IConsumerConfigurator<TConsumer> consumerConfigurator, IRegistrationContext context)
    {
        // Must stay false for command queues that must not receive publishes of the message type.
        endpointConfigurator.ConfigureConsumeTopology = _options.ConfigureConsumeTopology;
        if (endpointConfigurator is IRabbitMqReceiveEndpointConfigurator rabbitConfigurator)
        {
            SharedCommandTopology.Configure(rabbitConfigurator, _sendExchangeName, _options.PrefetchCount);
        }
    }
}
