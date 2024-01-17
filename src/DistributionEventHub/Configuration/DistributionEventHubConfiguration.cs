using MassTransit;
using MassTransit.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration;

/// <summary>
///     Basic configuration of the distribution event hub.
/// </summary>
internal class DistributionEventHubConfiguration : IDistributionEventHubConfiguration
{
    private readonly ServiceCollectionBusConfigurator _busConfigurator;
    private readonly IServiceCollection _serviceCollection;

    public DistributionEventHubConfiguration(IServiceCollection serviceCollection)
    {
        _serviceCollection = serviceCollection;
        _busConfigurator = new ServiceCollectionBusConfigurator(serviceCollection);
    }

    /// <summary>
    ///     Use the publish message scheduler to schedule messages using the Hangfire scheduler
    /// </summary>
    public bool UsePublishMessageScheduler { get; private set; }

    /// <summary>
    ///     Gets or sets the unique service name.
    /// </summary>
    public string UniqueServiceAddress { get; set; } = string.Empty;

    public void AddCommandClient<TRequest>(string commandName, TimeSpan? timeout = default)
        where TRequest : class
    {
        var requestTimeout = timeout ?? RequestTimeout.Default;
        _busConfigurator.AddRequestClient<TRequest>(new Uri($"queue:{commandName}?temporary=true"), requestTimeout);
        _serviceCollection.AddScoped<ICommandClient<TRequest>, CommandClient<TRequest>>();
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
        _serviceCollection.AddScoped<TConsumer>();
    }

    public void AddBroadcastEventConsumer<TConsumer, TMessage>()
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class
    {
        _busConfigurator
            .AddConsumer<DistributedConsumer<TConsumer, TMessage>,
                BroadcastEventConsumerDefinition<DistributedConsumer<TConsumer, TMessage>>>();
        _serviceCollection.AddScoped<TConsumer>();
    }

    public void AddRoutedEventConsumer<TConsumer, TMessage>()
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class
    {
        _busConfigurator
            .AddConsumer<DistributedConsumer<TConsumer, TMessage>,
                RoutedEventConsumerDefinition<DistributedConsumer<TConsumer, TMessage>>>();
        _serviceCollection.AddScoped<TConsumer>();
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