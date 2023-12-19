using Meshmakers.Octo.Common.DistributionEventHub.Payloads;
using Meshmakers.Octo.Common.DistributionEventHub.Repository;
using Meshmakers.Octo.Common.DistributionEventHub.Sagas;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Implements a distributed cache with pub sub mechanisms using REDIS
/// </summary>
// ReSharper disable once ClassNeverInstantiated.Global
internal class DistributedCacheService : IDistributedCacheService
{
    private readonly IRepositoryClient _repositoryClient;
    private readonly ITenantResolver _tenantResolver;

    /// <summary>
    ///     Constructor
    /// </summary>
    public DistributedCacheService(IRepositoryClient repositoryClient, ITenantResolver tenantResolver)
    {
        _repositoryClient = repositoryClient;
        _tenantResolver = tenantResolver;
    }

    /// <inheritdoc />
    public async Task<string> CacheStreamAsync(string? tenantId, Stream stream, string contentType, string fileName,
        TimeSpan? expiry = null)
    {
        var repositoryName = await _tenantResolver.GetRepositoryNameAsync(tenantId).ConfigureAwait(false);
        var persistentRepository = await _repositoryClient.GetRepositoryAsync(repositoryName).ConfigureAwait(false);
        return await persistentRepository.UploadBinaryAsync(stream, contentType, fileName, DateTime.Now + expiry).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task DeleteCacheStreamAsync(string? tenantId, string cacheStreamKey)
    {
        var repositoryName = await _tenantResolver.GetRepositoryNameAsync(tenantId).ConfigureAwait(false);
        var persistentRepository = await _repositoryClient.GetRepositoryAsync(repositoryName).ConfigureAwait(false);
        await persistentRepository.DeleteBinaryAsync(cacheStreamKey).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<CacheStream?> GetCacheStreamAsync(string? tenantId, string cacheStreamKey)
    {
        var repositoryName = await _tenantResolver.GetRepositoryNameAsync(tenantId).ConfigureAwait(false);
        var persistentRepository = await _repositoryClient.GetRepositoryAsync(repositoryName).ConfigureAwait(false);
        var downloadInfo = await persistentRepository.DownloadBinaryAsync(cacheStreamKey).ConfigureAwait(false);
        if (downloadInfo == null)
        {
            return null;
        }

        return new CacheStream { ContentType = downloadInfo.ContentType, Stream = downloadInfo.Stream, FileName = downloadInfo.Filename };
    }

    /// <summary>
    ///     Returns true when the channel is connected
    /// </summary>
    public bool IsConnected => _repositoryClient.IsConnected;
}