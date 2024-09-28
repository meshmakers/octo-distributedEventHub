using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

// ReSharper disable once ClassNeverInstantiated.Global
internal class RoutedEventConsumerDefinition<TConsumer, TMessage> : ConsumerDefinition<TConsumer>
    where TConsumer : class, IConsumer
    where TMessage : class
{
    public RoutedEventConsumerDefinition()
    {
        EndpointName = typeof(TMessage).FullName ?? "Unknown";
    }
    
    protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
        IConsumerConfigurator<TConsumer> consumerConfigurator,
        IRegistrationContext context)
    {
    }
}