using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.Options;

namespace Meshmakers.Octo.Common.DistributionEventHub.Sagas.Defaults;

/// <summary>
/// Default tenant resolver
/// </summary>
public class DefaultTenantResolver : ITenantResolver
{
    private readonly IOptions<DistributionEventHubOptions> _options;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="options"></param>
    public DefaultTenantResolver(IOptions<DistributionEventHubOptions> options)
    {
        _options = options;
    }
    
    /// <inheritdoc />
    public Task<string> GetRepositoryNameAsync(ConsumeContext consumeContext)
    {
        return null!;
    }

    /// <inheritdoc />
    public Task<string> GetRepositoryNameAsync(string? tenantId = null)
    {
        return Task.FromResult(tenantId ?? _options.Value.SystemDatabaseName);
    }
}