using Meshmakers.Octo.Common.DistributionEventHub.Repository;
using MongoDB.Driver.GridFS;

namespace Meshmakers.Octo.Common.DistributionEventHub.MongoDB.Repository;

internal class DownloadInfo : IDownloadInfo
{
    private readonly GridFSFileInfo _fsFileInfo;

    public DownloadInfo(GridFSFileInfo fsFileInfo)
    {
        _fsFileInfo = fsFileInfo;
    }

    public string ContentType => _fsFileInfo.Metadata.GetValue(CacheCommon.ContentType).AsString;
    public string BinaryId => _fsFileInfo.Id.ToString();
    public string Filename => _fsFileInfo.Filename;
    public DateTime UploadDateTime => _fsFileInfo.UploadDateTime;
    public long Length => _fsFileInfo.Length;
}