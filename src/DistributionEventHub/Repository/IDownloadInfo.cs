namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

/// <summary>
///     Generic download info
/// </summary>
public interface IDownloadInfo
{
    /// <summary>
    ///     Returns the used content type during upload
    /// </summary>
    public string ContentType { get; }

    /// <summary>
    ///     Returns the object id of the binary
    /// </summary>
    public string BinaryId { get; }

    /// <summary>
    ///     Returns the file name
    /// </summary>
    public string Filename { get; }

    /// <summary>
    ///     Returns upload date/time
    /// </summary>
    public DateTime UploadDateTime { get; }

    /// <summary>
    ///     Returns the lengths of the binary
    /// </summary>
    public long Length { get; }
}