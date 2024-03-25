using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Repository;
using Meshmakers.Octo.Common.DistributionEventHub.Repository.MongoDb;
using Meshmakers.Octo.Common.DistributionEventHub.Sagas;
using Meshmakers.Octo.Common.DistributionEventHub.Sagas.Defaults;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection.Extensions;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     DI extension methods for adding distributed cache with pub sub
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds a distributed cache with pub sub mechanisms to the service collection
    /// </summary>
    /// <param name="services">The current service collection</param>
    /// <param name="setupAction">
    ///     An <see cref="T:System.Action`1" /> to configure the provided
    ///     <see cref="T:Meshmakers.Octo.Backend.Common.DistributionEventHub.DistributeCacheWithPubSubOptions" />.
    /// </param>
    /// <param name="configurationAction">Basic configuration of the distribution event hub.</param>
    /// <returns>
    ///     The <see cref="T:Microsoft.SagaRegistrationConfiguratorExtensions.DependencyInjection.IServiceCollection" /> so that additional calls
    ///     can
    ///     be chained.
    /// </returns>
    public static IServiceCollection AddDistributionEventHubWithOptions(this IServiceCollection services,
        Action<DistributionEventHubOptions> setupAction, Action<IDistributionEventHubConfiguration> configurationAction)
    {
        services.AddOptions();
        services.Configure(setupAction);

        AddDistributionEventHub(services, configurationAction);

        return services;
    }

    /// <summary>
    ///     Adds a distributed cache with pub sub mechanisms to the service collection
    /// </summary>
    /// <param name="services">The current service collection</param>
    /// <param name="configurationAction">Basic configuration of the distribution event hub.</param>
    /// <returns>
    ///     The <see cref="T:Microsoft.SagaRegistrationConfiguratorExtensions.DependencyInjection.IServiceCollection" /> so that additional calls
    ///     can
    ///     be chained.
    /// </returns>
    public static IServiceCollection AddDistributionEventHub(this IServiceCollection services,
        Action<IDistributionEventHubConfiguration> configurationAction)
    {
        services.AddOptions();

        var configuration = new DistributionEventHubConfiguration(services);
        configurationAction.Invoke(configuration);

        if (string.IsNullOrWhiteSpace(configuration.UniqueServiceAddress))
        {
            throw DistributedOperationFailedException.NoUniqueServiceAddress();
        }

        services.ConfigureOptions<ConfigureRabbitMqTransportOptions>();

        services.Add(ServiceDescriptor.Singleton<IDistributedCacheService, DistributedCacheService>());
        services.TryAddSingleton<ITenantResolver, DefaultTenantResolver>();
        services.TryAddSingleton<IRepositoryClient, RepositoryClient>();
        services.TryAddSingleton<IDistributionEventHubService, DistributionEventHubService>();
        services.AddSingleton<IBroadcastServiceAddress>(_ => new BroadcastServiceAddress(configuration.UniqueServiceAddress));
        services.AddTransient<IEventHubControl, EventHubControl>();

        services.AddMassTransit();
        if (!configuration.AutomaticallyStartBusDuringStartup)
        {
            services.RemoveMassTransitHostedService();
        }

        configuration.ConfigureMassTransit(x =>
        {
            var schedulerEndpoint = new Uri(configuration.SchedulerEndpointAddress);
            x.AddMessageScheduler(schedulerEndpoint);

            x.UsingRabbitMq((context, cfg) =>
            {
                if (configuration.UsePublishMessageScheduler)
                {
                    cfg.UsePublishMessageScheduler();
                }

                cfg.UseMessageScheduler(schedulerEndpoint);
                cfg.ConfigureEndpoints(context);
            });
        });

        return services;
    }
}