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
    Task<string> CacheStreamAsync(string tenantId, Stream stream, string contentType, string fileName, TimeSpan? expiry = null);

    /// <summary>
    ///     Deletes a cached stream
    /// </summary>
    /// <param name="tenantId">The tenant id</param>
    /// <param name="cacheStreamKey">The key identifying the stream</param>
    /// <returns></returns>
    Task DeleteCacheStreamAsync(string tenantId, string cacheStreamKey);

    /// <summary>
    ///     Retrieves a cached stream
    /// </summary>
    /// <param name="tenantId">The tenant id</param>
    /// <param name="cacheStreamKey">The key identifying the stream</param>
    /// <returns></returns>
    Task<CacheStream?> GetCacheStreamAsync(string tenantId, string cacheStreamKey);
}