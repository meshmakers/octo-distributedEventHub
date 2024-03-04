using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository.MongoDb;

internal class Repository : IRepository
{
    private readonly GridFSBucket _bucket;
    private readonly Dictionary<Type, string> _collectionNameMapping = new();
    private readonly IMongoDatabase _mongoDatabase;

    public Repository(IMongoDatabase mongoDatabase)
    {
        _mongoDatabase = mongoDatabase;
        _bucket = new GridFSBucket(mongoDatabase, new GridFSBucketOptions
        {
            WriteConcern = WriteConcern.WMajority,
            ReadPreference = ReadPreference.SecondaryPreferred
        });
    }

    public IRepositoryCollection<TKey, TDocument> GetCollection<TKey, TDocument>(
        string? suffix = null)
        where TKey : notnull
        where TDocument : class, new()
    {
        var name = GetCollectionName<TDocument>(suffix);

        return new RepositoryCollection<TKey, TDocument>(_mongoDatabase.GetCollection<TDocument>(name));
    }

    public async Task<string> UploadBinaryAsync(Stream stream, string contentType, string fileName, DateTime? expiry,
        CancellationToken cancellationToken = default)
    {
        var cacheStreamKey = ObjectId.GenerateNewId();
        var options = new GridFSUploadOptions
        {
            Metadata = new BsonDocument
            {
                { CacheCommon.ContentType, contentType },
                { CacheCommon.ExpiryDateTime, expiry }
            }
        };

        await _bucket.UploadFromStreamAsync(cacheStreamKey, fileName, stream, options, cancellationToken).ConfigureAwait(false);

        return cacheStreamKey.ToString();
    }

    public async Task<string> UploadWithReplaceByFileNameBinaryAsync(Stream stream, string contentType, string fileName,
        CancellationToken cancellationToken = default)
    {
        var downloadInfo = await GetBinaryByFileNameAsync(fileName, cancellationToken).ConfigureAwait(false);
        if (downloadInfo != null)
        {
            await _bucket.DeleteAsync(downloadInfo.BinaryId, cancellationToken).ConfigureAwait(false);
        }
        
        var cacheStreamKey = ObjectId.GenerateNewId();
        var options = new GridFSUploadOptions
        {
            Metadata = new BsonDocument
            {
                { CacheCommon.ContentType, contentType }
            }
        };
        await _bucket.UploadFromStreamAsync(cacheStreamKey, fileName, stream, options, cancellationToken).ConfigureAwait(false);
        
        return cacheStreamKey.ToString();
    }

    public async Task DeleteBinaryAsync(string cacheStreamKey, CancellationToken cancellationToken = default)
    {
        await _bucket.DeleteAsync(cacheStreamKey, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IDownloadInfo?> GetBinaryByIdAsync(string cacheStreamKey, CancellationToken cancellationToken = default)
    {
        var filter = Builders<GridFSFileInfo>.Filter.Eq("_id", cacheStreamKey);
        var asyncCursor = await _bucket.FindAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
        var gridFsFileInfo = await asyncCursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        return new DownloadInfo(gridFsFileInfo);
    }
    
    public async Task<IDownloadInfo?> GetBinaryByFileNameAsync(string fileName, CancellationToken cancellationToken = default)
    {
        var filter = Builders<GridFSFileInfo>.Filter.Eq("Filename", fileName);
        var asyncCursor = await _bucket.FindAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
        var gridFsFileInfo = await asyncCursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        return new DownloadInfo(gridFsFileInfo);
    }

    public async Task<IDownloadStreamHandler?> DownloadBinaryAsync(ObjectId id, CancellationToken cancellationToken = default)
    {
        var gridFsDownloadStream =
            await _bucket.OpenDownloadStreamAsync(id, cancellationToken: cancellationToken).ConfigureAwait(false);

        return new DownloadStreamHandler(gridFsDownloadStream);
    }
    
    private string GetCollectionName<T>(string? suffix = null) where T : class, new()
    {
        if (!_collectionNameMapping.TryGetValue(typeof(T), out var name))
        {
            name = typeof(T).GetMostInnerBaseType().Name;
            _collectionNameMapping.Add(typeof(T), name);
        }

        if (!string.IsNullOrEmpty(suffix))
        {
            return name + "_" + suffix;
        }

        return name;
    }
}