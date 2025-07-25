namespace Meshmakers.Octo.Common.DistributionEventHub;

/// <summary>
///     Distributed operation failed exception
/// </summary>
public class DistributedOperationFailedException : DistributionException
{
    /// <summary>
    ///     Constructor
    /// </summary>
    public DistributedOperationFailedException()
    {
    }

    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="message">Exception message</param>
    public DistributedOperationFailedException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="message">Exception message</param>
    /// <param name="inner">Inner exception</param>
    public DistributedOperationFailedException(string message, Exception inner) : base(message, inner)
    {
    }

    internal static DistributedOperationFailedException CreateCommandFailed(string commandName, Exception inner)
    {
        return new DistributedOperationFailedException(
            $"Command {commandName} failed.", inner);
    }

    internal static Exception NoUniqueServiceAddress()
    {
        return new DistributedOperationFailedException(
            "Unique service address is not set. Please set the UniqueServiceAddress property.");
    }

    internal static Exception NoInstancePrefix()
    {
        return new DistributedOperationFailedException(
            "Instance prefix is not set. Please set the InstancePrefix property in the configuration.");
    }
}