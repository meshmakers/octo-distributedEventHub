using System.Linq.Expressions;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

/// <summary>
/// Interface for repository collections
/// </summary>
public interface IRepositoryCollection<TKey, TDocument>
    where TKey : notnull
    where TDocument : class, new()
{
    /// <summary>Finds a document by the given expression</summary>
    /// <param name="session">The session object</param>
    /// <param name="expression">Filter expression</param>
    /// <returns>The document</returns>
    Task<TDocument?> FindSingleOrDefaultAsync(
        IRepositorySession session,
        Expression<Func<TDocument, bool>> expression);
    
    /// <summary>Finds a document by the given expression</summary>
    /// <param name="session">The session object</param>
    /// <param name="expression">Filter expression</param>
    /// <param name="skip">Amount of documents to skip</param>
    /// <param name="take">Maximum amount of documents to return</param>
    /// <returns>List of documents</returns>
    Task<ICollection<TDocument>> FindManyAsync(
        IRepositorySession session,
        Expression<Func<TDocument, bool>> expression,
        int? skip = null,
        int? take = null);
    
    /// <summary>Inserts a new document into the collection</summary>
    /// <param name="session">The session object</param>
    /// <param name="document">The document to insert</param>
    /// <returns></returns>
    Task InsertOneAsync(IRepositorySession session, TDocument document);
}