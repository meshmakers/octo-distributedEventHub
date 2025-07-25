using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.DependencyInjection;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Sagas;
using Meshmakers.Octo.Common.DistributionEventHub.Sagas.Defaults;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

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
    public static IDistributedEventHubBuilder AddDistributionEventHubWithOptions(this IServiceCollection services,
        Action<DistributionEventHubOptions> setupAction, Action<IDistributionEventHubConfiguration> configurationAction)
    {
        services.AddOptions();
        services.Configure(setupAction);

        AddDistributionEventHub(services, configurationAction);

        return new DistributedEventHubBuilder(services);
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
    public static IDistributedEventHubBuilder AddDistributionEventHub(this IServiceCollection services,
        Action<IDistributionEventHubConfiguration> configurationAction)
    {
        services.AddOptions();

        var configuration = new DistributionEventHubConfiguration(services);
        configurationAction.Invoke(configuration);

        // Check if the instance prefix is set in the options
        var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetService<IOptions<DistributionEventHubOptions>>();
        if (options?.Value.InstancePrefix != null)
        {
            configuration.InstancePrefix = options.Value.InstancePrefix;
        }

        if (string.IsNullOrWhiteSpace(configuration.InstancePrefix))
        {
            throw DistributedOperationFailedException.NoInstancePrefix();
        }

        if (string.IsNullOrWhiteSpace(configuration.UniqueServiceAddress))
        {
            throw DistributedOperationFailedException.NoUniqueServiceAddress();
        }

        services.ConfigureOptions<ConfigureRabbitMqTransportOptions>();

        services.TryAddSingleton<ITenantResolver, DefaultTenantResolver>();
        services.TryAddSingleton<IDistributionEventHubService, DistributionEventHubService>();
        services.AddSingleton<IBroadcastServiceAddress>(_ =>
            new BroadcastServiceAddress(configuration.UniqueServiceAddress, configuration.InstancePrefix));
        services.AddTransient<IEventHubControl, EventHubControl>();


        services.AddMassTransit();
        if (!configuration.AutomaticallyStartBusDuringStartup)
        {
            services.RemoveMassTransitHostedService();
        }

        configuration.ConfigureMassTransit(x =>
        {
            var prefixedSchedulerAddress = CacheCommon.ApplyInstancePrefixToUri(configuration.InstancePrefix,
                new Uri(configuration.SchedulerEndpointAddress));
            x.AddMessageScheduler(prefixedSchedulerAddress);

            x.UsingRabbitMq((context, cfg) =>
            {
                try
                {
                    if (configuration.UsePublishMessageScheduler)
                    {
                        cfg.UsePublishMessageScheduler();
                    }

                    cfg.UseMessageScheduler(prefixedSchedulerAddress);

                    cfg.MessageTopology.SetEntityNameFormatter(
                        new PrefixEntityNameFormatter(cfg.MessageTopology.EntityNameFormatter,
                            CacheCommon.GetInstancePrefix(configuration.InstancePrefix)));

                    cfg.ConfigureEndpoints(context,
                        new DefaultEndpointNameFormatter(CacheCommon.GetInstancePrefix(configuration.InstancePrefix),
                            false));
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            });
        });

        return new DistributedEventHubBuilder(services);
    }
}