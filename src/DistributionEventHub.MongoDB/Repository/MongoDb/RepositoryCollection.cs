using System.Linq.Expressions;
using Meshmakers.Octo.Common.DistributionEventHub.Repository;
using MongoDB.Driver;

namespace Meshmakers.Octo.Common.DistributionEventHub.MongoDB.Repository.MongoDb;

internal class RepositoryCollection<TKey, TDocument> : IRepositoryCollection<TKey, TDocument>
    where TKey : notnull
    where TDocument : class, new()
{
    private readonly IMongoCollection<TDocument> _mongoCollection;

    public RepositoryCollection(IMongoCollection<TDocument> mongoCollection)
    {
        _mongoCollection = mongoCollection;
    }

    public async Task<TDocument?> FindSingleOrDefaultAsync(IRepositorySession session, Expression<Func<TDocument, bool>> expression)
    {
        try
        {
            return await (await _mongoCollection.FindAsync(((IRepositorySessionInternal)session).SessionHandle,
                expression).ConfigureAwait(false)).SingleOrDefaultAsync().ConfigureAwait(false);
        }
        catch (MongoException e)
        {
            throw new DistributedOperationFailedException(e.Message, e);
        }
    }

    public async Task<ICollection<TDocument>> FindManyAsync(IRepositorySession session,
        Expression<Func<TDocument, bool>> expression, int? skip = null, int? take = null)
    {
        try
        {
            var cursor = await _mongoCollection.FindAsync(((IRepositorySessionInternal)session).SessionHandle,
                expression,
                new FindOptions<TDocument> { Skip = skip, Limit = take }).ConfigureAwait(false);
            return await cursor.ToListAsync().ConfigureAwait(false);
        }
        catch (MongoException e)
        {
            throw new DistributedOperationFailedException(e.Message, e);
        }
    }

    public async Task InsertOneAsync(IRepositorySession session, TDocument document)
    {
        try
        {
            await _mongoCollection.InsertOneAsync(((IRepositorySessionInternal)session).SessionHandle, document).ConfigureAwait(false);
        }
        catch (MongoWriteException ex)
        {
            HandleWriteException<TDocument>(ex);
            throw;
        }
        catch (MongoException e)
        {
            throw new DistributedOperationFailedException(e.Message, e);
        }
    }

    private void HandleWriteException<T>(MongoWriteException ex)
    {
        if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            throw new DistributedOperationFailedException($"Error adding item of type {nameof(T)}", ex);
        }

        throw new DistributedOperationFailedException("Operation was not completed.", ex);
    }
}