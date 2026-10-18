namespace Meshmakers.Octo.Common.DistributionEventHub;

/// <summary>
///     Thrown when a command sent with the <c>mandatory</c> flag was returned by the broker because no queue is
///     bound to the command exchange (no consumer is currently registered for the command).
/// </summary>
public class CommandNotRoutableException : DistributionException
{
    /// <summary>
    ///     Constructor
    /// </summary>
    public CommandNotRoutableException()
    {
    }

    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="message">Exception message</param>
    public CommandNotRoutableException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="message">Exception message</param>
    /// <param name="inner">Inner exception</param>
    public CommandNotRoutableException(string message, Exception inner) : base(message, inner)
    {
    }

    internal static CommandNotRoutableException Create(string commandName, string commandAddress, Exception inner)
    {
        return new CommandNotRoutableException(
            $"Command {commandName} could not be routed to '{commandAddress}': the broker returned the message because no queue is bound.",
            inner);
    }
}
