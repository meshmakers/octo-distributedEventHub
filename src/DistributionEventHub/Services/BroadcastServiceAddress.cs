namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Represents the context of a service.
/// </summary>
internal class BroadcastServiceAddress : IBroadcastServiceAddress
{
    /// <summary>
    ///     Creates a new instance of the <see cref="BroadcastServiceAddress" /> class.
    /// </summary>
    /// <param name="serviceName">A unique name for the service.</param>
    /// <param name="instancePrefix">The instance prefix for multi-instance deployments.</param>
    public BroadcastServiceAddress(string serviceName, string instancePrefix)
    {
        ServiceName = serviceName;
        InstanceId = Guid.NewGuid().ToString();
        InstancePrefix = instancePrefix;
    }

    /// <inheritdoc />
    public string ServiceName { get; }

    /// <inheritdoc />
    public string InstanceId { get; }

    /// <inheritdoc />
    public string InstancePrefix { get; }
}