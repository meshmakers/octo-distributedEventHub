using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration;

/// <summary>
///     Interface for the configuration of the distribution event hub.
/// </summary>
public interface IDistributionEventHubConfiguration
{
    /// <summary>
    ///     Gets or sets the unique service address of this type of service
    /// </summary>
    string UniqueServiceAddress { get; set; }

    /// <summary>
    ///     Adds a bidirectional command to the DI container, by providing the command name and the
    ///     request type. A request client is added to the DI container.
    /// </summary>
    /// <param name="commandName">Unique name of command (e. g. my-command)</param>
    /// <param name="timeout">The default timeout of the command (default 30 seconds)</param>
    /// <typeparam name="TRequest">The request class of the commands</typeparam>
    void AddCommandClient<TRequest>(string commandName, TimeSpan? timeout = default)
        where TRequest : class;

    /// <summary>
    ///     Adds a command consumer to the DI container, by providing the command name and the consumer type.
    /// </summary>
    /// <param name="commandName">Unique name of command (e. g. my-command)</param>
    /// <typeparam name="TConsumer">The consumer type class</typeparam>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    void AddCommandConsumer<TConsumer, TMessage>(string commandName)
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class;

    /// <summary>
    ///     Adds a broadcast event consumer to the DI container, by providing the service name and the consumer type.
    ///     Broadcast events are received by all instances of services.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type class</typeparam>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    void AddBroadcastEventConsumer<TConsumer, TMessage>()
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class;

    /// <summary>
    ///     Adds a direct message consumer to the DI container, by providing the service name and the consumer type.
    ///     Direct messages are received by one instance of a service.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type class</typeparam>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    void AddRoutedEventConsumer<TConsumer, TMessage>()
        where TConsumer : class, IDistributedConsumer<TMessage>
        where TMessage : class;

    /// <summary>
    ///     Adds a SagaStateMachine to the registry, using the factory method, and updates the registrar prior to registering so that the default
    ///     saga registrar isn't notified.
    /// </summary>
    /// <param name="configure"></param>
    /// <typeparam name="TStateMachine"></typeparam>
    /// <typeparam name="T"></typeparam>
    ISagaRegistrationConfigurator<T> AddSagaStateMachine<TStateMachine, T>(
        Action<IRegistrationContext, ISagaConfigurator<T>>? configure = null)
        where TStateMachine : class, SagaStateMachine<T>
        where T : class, SagaStateMachineInstance;

    /// <summary>
    ///     Adds a message scheduler using Hangfire to the DI container
    /// </summary>
    void AddHangfireMessageScheduler();
}