using MassTransit;
using MassTransit.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration;

/// <summary>
///     Basic configuration of the distribution event hub.
/// </summary>

internal class DistributionEventHubConfiguration(IServiceCollection serviceCollection)
    : IDistributionEventHubConfiguration
{
    private readonly ServiceCollectionBusConfigurator _busConfigurator = new(serviceCollection);

    /// <summary>
    ///     Use the publishing message scheduler to schedule messages using the Hangfire scheduler
    /// </summary>
    public bool UsePublishMessageScheduler { get; private set; }
    
    /// <summary>
    /// Defines the endpoint address for the scheduler
    /// </summary>
    public string SchedulerEndpointAddress { get; set; } = "queue:scheduler";

    /// <summary>
    ///     Gets or sets the unique service name.
    /// </summary>
    public string UniqueServiceAddress { get; set; } = string.Empty;
    
    /// <summary>
    ///     Gets or sets a value indicating whether the bus should be automatically started during startup.
    /// </summary>
    /// <remarks>
    /// The bus can be started or stopped using the <see cref="IEventHubControl"/> service in manual mode.
    /// </remarks>
    public bool AutomaticallyStartBusDuringStartup { get; set; } = true;

    public void AddCommandClient<TRequest>(string commandName, TimeSpan? timeout = default)
        where TRequest : class
    {
        var requestTimeout = timeout ?? RequestTimeout.Default;
        _busConfigurator.AddRequestClient<TRequest>(new Uri($"exchange:{commandName}?temporary=true"), requestTimeout);
        serviceCollection.AddScoped<ICommandClient<TRequest>, CommandClient<TRequest>>();
    }
    
    public void AddRoutedCommandClient<TRequest>()
        where TRequest : class
    {
        serviceCollection.AddScoped<IRoutedCommandClient<TRequest>, RoutedCommandClient<TRequest>>();
    }

    public void AddCommandConsumer<TConsumer, TMessage>(string commandName)
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class
    {
        _busConfigurator.AddConsumer<DistributedConsumer<TConsumer, TMessage>>()
            .Endpoint(c =>
            {
                c.Name = commandName;
                c.Temporary = true;
                c.ConfigureConsumeTopology = false;
            });
        serviceCollection.AddScoped<TConsumer>();
    }
    
    public void AddBroadcastEventConsumer<TConsumer, TMessage>()
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class
    {
        _busConfigurator
            .AddConsumer<DistributedConsumer<TConsumer, TMessage>,
                BroadcastEventConsumerDefinition<DistributedConsumer<TConsumer, TMessage>>>();
        serviceCollection.AddScoped<TConsumer>();
    }
    
    public void AddRoutedEventConsumer<TConsumer, TMessage>(string destinationAddress)
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class
    {
        EndpointConvention.Map<TMessage>(new Uri($"queue:{destinationAddress}"));
        _busConfigurator
            .AddConsumer<DistributedConsumer<TConsumer, TMessage>,
                RoutedEventConsumerDefinition<DistributedConsumer<TConsumer, TMessage>, TMessage>>()
            .Endpoint(c =>
            {
                c.Name = destinationAddress;
            });
        serviceCollection.AddScoped<TConsumer>();
    }


    public void AddRoutedEventConsumer<TConsumer, TMessage>()
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class
    {
        _busConfigurator
            .AddConsumer<DistributedConsumer<TConsumer, TMessage>,
                RoutedEventConsumerDefinition<DistributedConsumer<TConsumer, TMessage>, TMessage>>();
        serviceCollection.AddScoped<TConsumer>();
    }

    public ISagaRegistrationConfigurator<T>
        AddSagaStateMachine<TStateMachine, T>(Action<IRegistrationContext, ISagaConfigurator<T>>? configure = null)
        where TStateMachine : class, SagaStateMachine<T> where T : class, SagaStateMachineInstance
    {
        return _busConfigurator.AddSagaStateMachine<TStateMachine, T>();
    }

    public void AddHangfireMessageScheduler()
    {
        UsePublishMessageScheduler = true;

        _busConfigurator.AddPublishMessageScheduler();

        _busConfigurator.AddHangfireConsumers();
    }

    internal void ConfigureMassTransit(Action<IBusRegistrationConfigurator> action)
    {
        action.Invoke(_busConfigurator);
    }
}