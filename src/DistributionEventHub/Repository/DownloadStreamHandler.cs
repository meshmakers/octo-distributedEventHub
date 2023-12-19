using Meshmakers.Octo.ConstructionKit.Contracts;
using MongoDB.Driver.GridFS;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

internal class DownloadStreamHandler : IDownloadStreamHandler
{
    private readonly GridFSDownloadStream _stream;

    public DownloadStreamHandler(GridFSDownloadStream stream)
    {
        _stream = stream;
    }

    public void Dispose()
    {
        _stream.Dispose();
    }

    public OctoObjectId Id => _stream.FileInfo.Id.ToOctoObjectId();
    public string ContentType => _stream.FileInfo.Metadata.GetValue(CacheCommon.ContentType).AsBsonValue.AsString;
    public DateTime UploadDateTime => _stream.FileInfo.UploadDateTime;
    public Stream Stream => _stream;
    public string Filename => _stream.FileInfo.Filename;

    public void Close()
    {
        _stream.Close();
    }
}