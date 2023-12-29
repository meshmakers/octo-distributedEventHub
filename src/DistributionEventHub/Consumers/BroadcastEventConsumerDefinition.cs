using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;

namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

internal class BroadcastEventConsumerDefinition<TConsumer> : ConsumerDefinition<TConsumer>
    where TConsumer : class, IConsumer
{
    public BroadcastEventConsumerDefinition(IBroadcastServiceAddress serviceAddress)
    {
        EndpointName = string.Format(CacheCommon.ServiceEndpointPattern, serviceAddress.ServiceName);
    }
    
    protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator, IConsumerConfigurator<TConsumer> consumerConfigurator,
        IRegistrationContext context)
    {
    }
}