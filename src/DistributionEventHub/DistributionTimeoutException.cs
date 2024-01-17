using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub;

/// <summary>
///     Exception thrown when a command times out
/// </summary>
public class DistributionTimeoutException : DistributionException
{
    private DistributionTimeoutException()
    {
    }

    private DistributionTimeoutException(string message) : base(message)
    {
    }

    private DistributionTimeoutException(string message, Exception inner) : base(message, inner)
    {
    }

    internal static DistributionTimeoutException Create(string commandName, RequestTimeout requestTimeout, Exception inner)
    {
        return new DistributionTimeoutException(
            $"Command {commandName} timed out after {requestTimeout.Value.TotalSeconds} seconds.", inner);
    }
}