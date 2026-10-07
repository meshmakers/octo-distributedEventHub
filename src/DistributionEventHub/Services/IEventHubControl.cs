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
    ///     Registers a handler for a routed event message with the specified destination address, with
    ///     explicit consumption options (prefetch, concurrency, coalescing of a backlog — AB#5709).
    /// </summary>
    /// <param name="destinationAddress">destination address of the message (the queue name, without instance prefix)</param>
    /// <param name="handler">Handler for the message; receives the delivery context as second argument</param>
    /// <param name="options">Consumption options; see <see cref="RoutedEventConsumerOptions" /></param>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    /// <remarks>
    ///     The default implementation ignores <paramref name="options" /> and delegates to
    ///     <see cref="RegisterRoutedEventConsumer{TMessage}(string, Func{TMessage, Task})" />, so existing
    ///     implementations of this interface (test doubles) keep compiling. The event hub's own
    ///     implementation honours the options.
    /// </remarks>
    /// <exception cref="ArgumentException">The options contradict each other.</exception>
    EndpointHandle RegisterRoutedEventConsumer<TMessage>(string destinationAddress,
        Func<TMessage, RoutedEventDeliveryContext, Task> handler, RoutedEventConsumerOptions options)
        where TMessage : class
    {
        options.Validate();
        return RegisterRoutedEventConsumer<TMessage>(destinationAddress,
            message => handler(message, RoutedEventDeliveryContext.None));
    }
    
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