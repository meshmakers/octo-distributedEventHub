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
    /// Gets or sets the instance prefix for multi-instance OctoMesh deployments.
    /// </summary>
    public string InstancePrefix { get; set; } = "default";

    /// <summary>
    /// Defines the endpoint address for the scheduler
    /// </summary>
    public string SchedulerEndpointAddress { get; set; } = "queue:scheduler";

    /// <summary>
    ///     Gets or sets the unique service name.
    /// </summary>
    public string UniqueServiceAddress { get; set; } = string.Empty;

    /// <summary>
    /// Gets the endpoint name for the service.
    /// </summary>
    public string ServiceInstanceId { get; } = Guid.NewGuid().ToString();

    /// <summary>
    ///     Gets or sets a value indicating whether the bus should be automatically started during startup.
    /// </summary>
    /// <remarks>
    /// The bus can be started or stopped using the <see cref="IEventHubControl"/> service in manual mode.
    /// </remarks>
    public bool AutomaticallyStartBusDuringStartup { get; set; } = true;

    public void AddCommandClient<TRequest>(string commandName, TimeSpan? timeout = null)
        where TRequest : class
    {
        var requestTimeout = timeout ?? RequestTimeout.Default;
        var prefixedCommandName = CacheCommon.ApplyInstancePrefix(InstancePrefix, commandName);
        _busConfigurator.AddRequestClient<TRequest>(new Uri($"exchange:{prefixedCommandName}?temporary=true"), requestTimeout);
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
        var prefixedCommandName = CacheCommon.ApplyInstancePrefix(InstancePrefix, commandName);
        _busConfigurator.AddConsumer<DistributedConsumer<TConsumer, TMessage>>()
            .Endpoint(c =>
            {
                c.Name = prefixedCommandName;
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
            .AddConsumer<DistributedConsumer<TConsumer, TMessage>>().Endpoint(c =>
            {
                var baseEndpointName = string.Format(CacheCommon.ServiceEndpointPattern,
                    UniqueServiceAddress + "-" + ServiceInstanceId);
                c.Name = CacheCommon.ApplyInstancePrefix(InstancePrefix, baseEndpointName);
                c.Temporary = true;
            });
        serviceCollection.AddScoped<TConsumer>();
    }
    
    public void AddRoutedEventConsumer<TConsumer, TMessage>(string destinationAddress)
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class
    {
        var prefixedAddress = CacheCommon.ApplyInstancePrefix(InstancePrefix, destinationAddress);
        EndpointConvention.Map<TMessage>(new Uri($"queue:{prefixedAddress}"));
        // When explicit destination address is provided, don't use RoutedEventConsumerDefinition
        // as it would override the endpoint name with a message-type-based name.
        // Register the consumer directly with the explicit endpoint name instead.
        _busConfigurator
            .AddConsumer<DistributedConsumer<TConsumer, TMessage>>()
            .Endpoint(c =>
            {
                c.Name = prefixedAddress;
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

        // AB#5867: the scheduler endpoint is instance-scoped. MassTransit's HangfireEndpointDefinition
        // ignores the endpoint name formatter, so without this every instance consumed the same
        // "hangfire" queue and a schedule could be stored by another instance's bot service, where
        // RemoveRecurringJobsByScheduleGroup never reaches it. Read lazily: InstancePrefix may still be
        // set after this call.
        _busConfigurator.AddHangfireConsumers(o => o.QueueName = HangfireSchedulerTopology.GetQueueName(InstancePrefix));
        serviceCollection.AddBusObserver(sp => new LegacyHangfireSchedulerBindingRemover(
            sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqTransportOptions>>(),
            () => InstancePrefix,
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<LegacyHangfireSchedulerBindingRemover>>()));
    }

    internal void ConfigureMassTransit(Action<IBusRegistrationConfigurator> action)
    {
        action.Invoke(_busConfigurator);
    }
}