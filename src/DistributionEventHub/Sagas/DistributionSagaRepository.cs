using MassTransit;
using MassTransit.Context;
using Meshmakers.Octo.Common.DistributionEventHub.Repository;

namespace Meshmakers.Octo.Common.DistributionEventHub.Sagas;

/// <summary>
///     A basic implementation of a saga repository for mass transit
/// </summary>
/// <typeparam name="TSaga">Type of saga</typeparam>
/// <typeparam name="TKey">The key type</typeparam>
public class DistributionSagaRepository<TKey, TSaga> : ISagaRepository<TSaga>
    where TKey : notnull
    where TSaga : class, ISaga, new()
{
    private readonly IRepositoryClient _repositoryClient;
    private readonly ITenantResolver _tenantResolver;

    /// <summary>
    ///     Constructor
    /// </summary>
    public DistributionSagaRepository(IRepositoryClient repositoryClient, ITenantResolver tenantResolver)
    {
        _repositoryClient = repositoryClient;
        _tenantResolver = tenantResolver;
    }


    /// <inheritdoc />
    public void Probe(ProbeContext context)
    {
        context.Add("type", "MongoDB");
    }

    /// <inheritdoc />
    public async Task Send<T>(ConsumeContext<T> context, ISagaPolicy<TSaga, T> policy, IPipe<SagaConsumeContext<TSaga, T>> next)
        where T : class
    {
        var collection = await GetCollectionAsync(context).ConfigureAwait(false);

        // Start a MongoDB session for transaction support
        using (var session = await _repositoryClient.StartSessionAsync().ConfigureAwait(false))
        {
            try
            {
                var sagaId = context.CorrelationId ?? Guid.NewGuid();

                // Start transaction
                session.StartTransaction();

                var sagaInstances = await collection.FindManyAsync(session, x => x.CorrelationId == sagaId).ConfigureAwait(false);

                if (!sagaInstances.Any())
                {
                    await policy.Missing(context, next).ConfigureAwait(false);
                }
                else
                {
                    foreach (var instance in sagaInstances)
                    {
                        // Implement concurrency control and versioning logic here

                        var sagaConsumeContext = new DefaultSagaConsumeContext<TSaga, T>(context, instance);
                        await policy.Existing(sagaConsumeContext, next).ConfigureAwait(false);

                        // Additional logic for version increment or other state changes
                    }
                }

                // Implement concurrency control and versioning logic here

                // More code to handle saga instance

                // Commit the transaction
                await session.CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await session.AbortTransactionAsync().ConfigureAwait(false);
                // Handle exceptions
                throw;
            }
        }
    }

    /// <inheritdoc />
    public async Task SendQuery<T>(ConsumeContext<T> context, ISagaQuery<TSaga> query, ISagaPolicy<TSaga, T> policy,
        IPipe<SagaConsumeContext<TSaga, T>> next) where T : class
    {
        var collection = await GetCollectionAsync(context).ConfigureAwait(false);

        // Start a MongoDB session for transaction support
        using (var session = await _repositoryClient.StartSessionAsync().ConfigureAwait(false))
        {
            try
            {
                var sagaId = context.CorrelationId ?? Guid.NewGuid();

                // Start transaction
                session.StartTransaction();

                var sagaInstance = await collection.FindSingleOrDefaultAsync(session, x => x.CorrelationId == sagaId).ConfigureAwait(false);

                if (sagaInstance == null && policy.PreInsertInstance(context, out var newInstance))
                {
                    sagaInstance = newInstance;
                    await collection.InsertOneAsync(session, sagaInstance).ConfigureAwait(false);
                }


                if (sagaInstance == null)
                {
                    return;
                }
                // Implement concurrency control and versioning logic here

                var sagaConsumeContext = new DefaultSagaConsumeContext<TSaga, T>(context, sagaInstance);
                await policy.Existing(sagaConsumeContext, next).ConfigureAwait(false);

                // Additional logic for version increment or other state changes

                // Implement concurrency control and versioning logic here

                // More code to handle saga instance

                // Commit the transaction
                await session.CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await session.AbortTransactionAsync().ConfigureAwait(false);
                // Handle exceptions
                throw;
            }
        }
    }

    private async Task<IRepositoryCollection<TKey, TSaga>> GetCollectionAsync(ConsumeContext context)
    {
        var repositoryName = await _tenantResolver.GetRepositoryNameAsync(context).ConfigureAwait(false);
        var persistentRepository = await _repositoryClient.GetRepositoryAsync(repositoryName).ConfigureAwait(false);
        return persistentRepository.GetCollection<TKey, TSaga>();
    }
}