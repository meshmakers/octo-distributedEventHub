namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Interface for a service context, that describes the current service.
/// </summary>
internal interface IBroadcastServiceAddress
{
    /// <summary>
    ///     Returns the unique name of the service within the system.
    /// </summary>
    string ServiceName { get; }

    /// <summary>
    /// Gets the unique identifier for the service instance.
    /// </summary>
    string InstanceId { get; }
    
    /// <summary>
    ///     Returns the instance prefix for multi-instance deployments.
    /// </summary>
    string InstancePrefix { get; }
}