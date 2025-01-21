using Meshmakers.Octo.Common.DistributionEventHub.Configuration.DependencyInjection;
using Meshmakers.Octo.Common.DistributionEventHub.MongoDB.Repository.MongoDb;
using Meshmakers.Octo.Common.DistributionEventHub.Repository;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Meshmakers.Octo.Common.DistributionEventHub.MongoDB.Configuration.DependencyInjection
{
    /// <summary>
    ///     DI extension methods for adding mongodb distributed event hub
    /// </summary>
    public static class IDistributedEventHubBuilderExtensions
    {
        /// <summary>
        ///     Adds a distributed cache with pub sub mechanisms to the service collection
        /// </summary>
        /// <param name="builder">The current builder</param>
        /// <returns>
        ///     The <see cref="T:Meshmakers.Octo.Common.DistributionEventHub.Configuration.DependencyInjection.IDistributedEventHubBuilder" /> so that additional calls
        ///     can
        ///     be chained.
        /// </returns>
        public static IDistributedEventHubBuilder AddMongoDbRepository(this IDistributedEventHubBuilder builder)
        {
            builder.Services.TryAdd(ServiceDescriptor.Singleton<IDistributedCacheService, DistributedCacheService>());
            builder.Services.TryAddSingleton<IRepositoryClient, RepositoryClient>();

            return builder;
        }
    }
}