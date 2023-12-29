namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

/// <summary>
/// Interface to retrieve an distributed invoke.
/// </summary>
public interface IDistributedContext<out TMessage>
    where TMessage : class
{
    /// <summary>
    /// The message that was received
    /// </summary>
    TMessage Message { get; }
    
    /// <summary>
    /// Responds to the current message immediately, returning the Task for the
    /// sending message. The caller may choose to await the response to ensure it was sent, or
    /// allow the framework to wait for it (which will happen automatically before the message is acknowledged)
    /// </summary>
    /// <typeparam name="T">The type of the message to respond with.</typeparam>
    /// <param name="message">The message to send in response</param>
    Task RespondAsync<T>(T message)
        where T : class;
}