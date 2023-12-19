using MassTransit;
using MassTransit.Configuration;
using MassTransit.Saga;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Meshmakers.Octo.Common.DistributionEventHub.Sagas;

internal static class SagaRegistrationConfiguratorExtensions
{
    /// <summary>
    /// Adds an in-memory saga repository to the registration
    /// </summary>
    /// <param name="configurator"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static ISagaRegistrationConfigurator<T> DistributionEventHubRepository<T>(this ISagaRegistrationConfigurator<T> configurator)
        where T : class, ISaga
    {
        configurator.Repository(x => x.RegisterDistributionEventHubSagaRepository<T>());

        return configurator;
    }
    
    /// <summary>
    /// Register the InMemory saga repository for the specified saga type
    /// </summary>
    /// <param name="collection"></param>
    /// <typeparam name="T"></typeparam>
    public static void RegisterDistributionEventHubSagaRepository<T>(this IServiceCollection collection)
        where T : class, ISaga
    {
        collection.TryAddSingleton(new IndexedSagaDictionary<T>());
        collection.RegisterLoadSagaRepository<T, InMemorySagaRepositoryContextFactory<T>>();
        collection.RegisterQuerySagaRepository<T, InMemorySagaRepositoryContextFactory<T>>();
        collection.RegisterSagaRepository<T, IndexedSagaDictionary<T>, InMemorySagaConsumeContextFactory<T>, InMemorySagaRepositoryContextFactory<T>>();
    }
}