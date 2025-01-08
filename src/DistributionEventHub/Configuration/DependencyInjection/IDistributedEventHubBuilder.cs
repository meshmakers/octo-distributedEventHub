using Microsoft.Extensions.DependencyInjection;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration.DependencyInjection;

/// <summary>
/// Builder for the distributed event hub
/// </summary>
public interface IDistributedEventHubBuilder
{
    /// <summary>
    ///     Gets the services.
    /// </summary>
    /// <value>
    ///     The services.
    /// </value>
    IServiceCollection Services { get; }
}