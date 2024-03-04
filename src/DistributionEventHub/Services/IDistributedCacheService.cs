using Meshmakers.Octo.Common.DistributionEventHub.Payloads;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Interface of a distributed cache with pub sub mechanisms using REDIS
/// </summary>
public interface IDistributedCacheService
{
    /// <summary>
    ///     Returns true when the channel is connected
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    ///     Caches a stream for the given amount of time
    /// </summary>
    /// <param name="tenantId">The tenant id</param>
    /// <param name="stream">The file stream</param>
    /// <param name="contentType">Content type of the file</param>
    /// <param name="fileName">Original file name</param>
    /// <param name="expiry">The amount of time the stream gets cached</param>
    /// <returns>The key identifying the stream</returns>
    Task<string> CreateStreamAsync(string tenantId, Stream stream, string contentType, string fileName, TimeSpan? expiry = null);
    
    /// <summary>
    ///     Caches a stream and updates it if it already exists. The stream is cached indefinitely
    /// </summary>
    /// <param name="tenantId">The tenant id</param>
    /// <param name="stream">The file stream</param>
    /// <param name="contentType">Content type of the file</param>
    /// <param name="fileName">Original file name</param>
    /// <returns>The key identifying the stream</returns>
    Task<string> CreateOrUpdateStreamAsync(string tenantId, Stream stream, string contentType, string fileName);

    /// <summary>
    ///     Deletes a cached stream
    /// </summary>
    /// <param name="tenantId">The tenant id</param>
    /// <param name="cacheStreamKey">The key identifying the stream</param>
    /// <returns></returns>
    Task DeleteCacheStreamAsync(string tenantId, string cacheStreamKey);

    /// <summary>
    ///     Retrieves a cached stream by id of the stream
    /// </summary>
    /// <param name="tenantId">The tenant id</param>
    /// <param name="cacheStreamKey">The key identifying the stream</param>
    /// <returns></returns>
    Task<CacheStream?> GetCacheStreamByIdAsync(string tenantId, string cacheStreamKey);

    /// <summary>
    /// Retrieves a cached stream by file name
    /// </summary>
    /// <param name="tenantId"></param>
    /// <param name="fileName"></param>
    /// <returns></returns>
    Task<CacheStream?> GetCacheStreamByFileNameAsync(string tenantId, string fileName);
}