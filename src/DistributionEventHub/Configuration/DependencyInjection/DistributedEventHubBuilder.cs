using Microsoft.Extensions.DependencyInjection;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration.DependencyInjection;

internal class DistributedEventHubBuilder(IServiceCollection serviceCollection) : IDistributedEventHubBuilder
{
    public IServiceCollection Services { get; } = serviceCollection;
}