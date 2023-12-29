namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

/// <summary>
/// Interface to retrieve an distributed invoke.
/// </summary>
/// <typeparam name="TMessage">Type of the message</typeparam>
public interface IDistributedConsumer<in TMessage>
    where TMessage : class
{
    /// <summary>
    /// Consumes an incoming message
    /// </summary>
    /// <param name="context">Context of the incoming message</param>
    /// <returns></returns>
    Task ConsumeAsync(IDistributedContext<TMessage> context);
}