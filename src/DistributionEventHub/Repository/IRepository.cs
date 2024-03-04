using MongoDB.Bson;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

/// <summary>
///     Represents a persistent repository
/// </summary>
public interface IRepository
{
    /// <summary>
    ///     Gets a collection from the repository
    /// </summary>
    /// <param name="suffix">Optional suffix of collection name</param>
    /// <typeparam name="TKey">Type of key</typeparam>
    /// <typeparam name="TDocument">Type of collection entities</typeparam>
    /// <returns></returns>
    public IRepositoryCollection<TKey, TDocument> GetCollection<TKey, TDocument>(
        string? suffix = null)
        where TKey : notnull
        where TDocument : class, new();

    /// <summary>
    ///     Uploads a binary to the repository
    /// </summary>
    /// <param name="stream">The file stream</param>
    /// <param name="contentType">Content type of the file</param>
    /// <param name="fileName">The original file name</param>
    /// <param name="expiry">The amount of time the stream gets cached</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The cache stream key</returns>
    Task<string> UploadBinaryAsync(Stream stream, string contentType, string fileName, DateTime? expiry,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    ///     Uploads a binary to the repository
    /// </summary>
    /// <param name="stream">The file stream</param>
    /// <param name="contentType">Content type of the file</param>
    /// <param name="fileName">The original file name</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The cache stream key</returns>
    Task<string> UploadWithReplaceByFileNameBinaryAsync(Stream stream, string contentType, string fileName,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Deletes a binary from the repository
    /// </summary>
    /// <param name="cacheStreamKey">The key identifying the stream</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns></returns>
    Task DeleteBinaryAsync(string cacheStreamKey, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets a binary from the repository using the id
    /// </summary>
    /// <param name="cacheStreamKey">The key identifying the stream</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>if the binary is not found, NULL is returned</returns>
    Task<IDownloadInfo?> GetBinaryByIdAsync(string cacheStreamKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a binary from the repository using the file name
    /// </summary>
    /// <param name="fileName"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task<IDownloadInfo?> GetBinaryByFileNameAsync(string fileName, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Downloads a binary from the repository
    /// </summary>
    /// <param name="id">The id of the stream</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>if the binary is not found, NULL is returned</returns>
    Task<IDownloadStreamHandler?> DownloadBinaryAsync(ObjectId id, CancellationToken cancellationToken = default);
}