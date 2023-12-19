using Meshmakers.Octo.ConstructionKit.Contracts;
using MongoDB.Driver.GridFS;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

internal class DownloadInfo : IDownloadInfo
{
    private readonly GridFSFileInfo _fsFileInfo;

    public DownloadInfo(GridFSFileInfo fsFileInfo)
    {
        _fsFileInfo = fsFileInfo;
    }

    public string ContentType => _fsFileInfo.Metadata.GetValue(CacheCommon.ContentType).AsString;
    public OctoObjectId BinaryId => _fsFileInfo.Id.ToOctoObjectId();
    public string Filename => _fsFileInfo.Filename;
    public DateTime UploadDateTime => _fsFileInfo.UploadDateTime;
    public long Length => _fsFileInfo.Length;
}