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

    /// <summary>
    ///     Registers a handler for a command message on a shared command endpoint: processes registering the same
    ///     command name are competing consumers on one durable, auto-delete, non-exclusive queue that is bound to the
    ///     stable send exchange <c>{prefix}-{commandName}::send</c>.
    ///     Call it with <see cref="IDistributionEventHubService.GetCommandResponseAsync{TRequest,TResponse}(string, TRequest, CommandSendOptions, CancellationToken)" />
    ///     (the two options overloads form a pair; do not mix with the legacy overloads for the same command name).
    ///     Await <see cref="EndpointHandle.WaitUntilReadyAsync" /> before announcing the endpoint.
    /// </summary>
    /// <remarks>
    ///     The handler should never throw: the bus-level message retry also wraps dynamically connected endpoints,
    ///     so an exception escaping the handler causes the message to be redelivered (up to 4 executions).
    /// </remarks>
    /// <param name="commandName">Command name (queue name without the instance prefix)</param>
    /// <param name="handler">Handler for the message</param>
    /// <param name="options">Endpoint options</param>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    /// <returns>Handle to stop and remove the endpoint</returns>
    EndpointHandle RegisterCommandConsumer<TMessage>(string commandName, ExecuteCommandHandler<TMessage> handler,
        CommandConsumerOptions options) where TMessage : class
        => throw new NotSupportedException(
            "This IEventHubControl implementation does not support shared command endpoints.");

    /// <summary>
    ///     Same as <see cref="RegisterCommandConsumer{TMessage}(string, ExecuteCommandHandler{TMessage}, CommandConsumerOptions)" />,
    ///     but the handler also receives <see cref="CommandDeliveryInfo" /> so it can refuse to execute a message that
    ///     may have been processed before (broker redelivery after a lost acknowledgement, in-process retry).
    /// </summary>
    /// <param name="commandName">Command name (queue name without the instance prefix)</param>
    /// <param name="handler">Handler for the message</param>
    /// <param name="options">Endpoint options</param>
    /// <typeparam name="TMessage">Type of incoming message</typeparam>
    /// <returns>Handle to stop and remove the endpoint</returns>
    EndpointHandle RegisterCommandConsumer<TMessage>(string commandName, ExecuteDeliveredCommandHandler<TMessage> handler,
        CommandConsumerOptions options) where TMessage : class
        => throw new NotSupportedException(
            "This IEventHubControl implementation does not support shared command endpoints.");
}
