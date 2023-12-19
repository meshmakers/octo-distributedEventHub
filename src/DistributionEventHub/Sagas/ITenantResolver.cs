using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Sagas;

/// <summary>
/// Resolves the database name for a given tenant id
/// </summary>
public interface ITenantResolver
{
    /// <summary>
    /// Gets the database name for a given tenant id
    /// </summary>
    /// <param name="consumeContext">The consume context tenant is resolved</param>
    /// <returns>Database name</returns>
    Task<string> GetRepositoryNameAsync(ConsumeContext consumeContext);
    
    /// <summary>
    /// Gets the database name for a given tenant id
    /// </summary>
    /// <param name="tenantId">The tenant id if existing</param>
    /// <returns>Database name</returns>
    Task<string> GetRepositoryNameAsync(string? tenantId = null);
}