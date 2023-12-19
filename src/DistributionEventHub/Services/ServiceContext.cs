namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Represents the context of a service.
/// </summary>
public class ServiceContext : IServiceContext
{
    /// <summary>
    /// Creates a new instance of the <see cref="ServiceContext"/> class.
    /// </summary>
    /// <param name="serviceName">A unique name for the service.</param>
    public ServiceContext(string serviceName)
    {
        ServiceName = serviceName;
    }

    /// <inheritdoc />
    public string ServiceName { get; }
}