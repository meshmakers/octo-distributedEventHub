namespace Meshmakers.Octo.Common.DistributionEventHub;

/// <summary>
///     Base class for all distribution exceptions
/// </summary>
public abstract class DistributionException : Exception
{
    /// <inheritdoc />
    protected DistributionException()
    {
    }

    /// <inheritdoc />
    protected DistributionException(string message) : base(message)
    {
    }

    /// <inheritdoc />
    protected DistributionException(string message, Exception inner) : base(message, inner)
    {
    }
}