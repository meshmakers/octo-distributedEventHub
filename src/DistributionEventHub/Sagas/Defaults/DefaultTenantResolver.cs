using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Sagas.Defaults;

/// <summary>
/// Default tenant resolver
/// </summary>
public class DefaultTenantResolver : ITenantResolver
{
    /// <summary>
    /// Constructor
    /// </summary>
    public DefaultTenantResolver()
    {
    }
    
    /// <inheritdoc />
    public Task<string> GetRepositoryNameAsync(ConsumeContext consumeContext)
    {
        return null!;
    }

    /// <inheritdoc />
    public Task<string> GetRepositoryNameAsync(string tenantId)
    {
        return Task.FromResult(tenantId);
    }
}