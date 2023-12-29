namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Represents the context of a service.
/// </summary>
internal class BroadcastServiceAddress : IBroadcastServiceAddress
{
    /// <summary>
    /// Creates a new instance of the <see cref="BroadcastServiceAddress"/> class.
    /// </summary>
    /// <param name="serviceName">A unique name for the service.</param>
    public BroadcastServiceAddress(string serviceName)
    {
        ServiceName = serviceName;
    }

    /// <inheritdoc />
    public string ServiceName { get; }
}