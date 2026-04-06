namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Interface for event hub control, that allows to start and stop the event hub
///     Especially useful for console apps or tests
/// </summary>
public interface IEventHubControl
{
    /// <summary>
    ///     Starts the event hub
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns></returns>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Stops the event hub
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns></returns>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Registers a handler for a routed event message with the specified destination address.
    /// </summary>
    /// <param name="destinationAddress">destination address of the message</param>
    /// <param name="handler">Handler for the message</param>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    EndpointHandle RegisterRoutedEventConsumer<TMessage>(string destinationAddress, Func<TMessage, Task> handler)
        where TMessage : class;
    
    /// <summary>
    ///     Registers a handler for a routed event message with the specified destination address.
    /// </summary>
    /// <param name="handler">Handler for the message</param>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    EndpointHandle RegisterRoutedEventConsumer<TMessage>(Func<TMessage, Task> handler)
        where TMessage : class;
    
    /// <summary>
    ///     Registers a handler for a routed event message bound to a topic exchange with the specified routing key.
    /// </summary>
    /// <param name="exchangeName">Name of the topic exchange to bind to</param>
    /// <param name="routingKey">Routing key pattern for filtering messages</param>
    /// <param name="handler">Handler for the message</param>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    EndpointHandle RegisterRoutedEventConsumer<TMessage>(string exchangeName, string routingKey,
        Func<TMessage, Task> handler) where TMessage : class;

    /// <summary>
    ///     Registers a handler for a command message with the specified command name.
    /// </summary>
    /// <param name="commandName"></param>
    /// <param name="handler"></param>
    /// <typeparam name="TMessage"></typeparam>
    /// <returns></returns>
    EndpointHandle RegisterCommandConsumer<TMessage>(string commandName, ExecuteCommandHandler<TMessage> handler)
        where TMessage : class;
}