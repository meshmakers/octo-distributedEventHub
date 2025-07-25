using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;

namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

// ReSharper disable once ClassNeverInstantiated.Global
internal class RoutedEventConsumerDefinition<TConsumer, TMessage> : ConsumerDefinition<TConsumer>
    where TConsumer : class, IConsumer
    where TMessage : class
{
    public RoutedEventConsumerDefinition(IBroadcastServiceAddress serviceAddress)
    {
        var baseEndpointName = string.Format(CacheCommon.ServiceEndpointPattern, typeof(TMessage).FullName);
        EndpointName = CacheCommon.ApplyInstancePrefix(serviceAddress.InstancePrefix, baseEndpointName);
    }

    protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
        IConsumerConfigurator<TConsumer> consumerConfigurator,
        IRegistrationContext context)
    {
    }
}