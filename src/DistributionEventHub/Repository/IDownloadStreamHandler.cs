using MongoDB.Bson;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

/// <summary>
///     Handles the download of a stream from a persistent storage
/// </summary>
public interface IDownloadStreamHandler : IDisposable
{
    /// <summary>
    ///     Returns the object id of the binary
    /// </summary>
    ObjectId Id { get; }

    /// <summary>
    ///     Returns the used content type during upload
    /// </summary>
    string ContentType { get; }


    /// <summary>
    ///     Returns upload date/time
    /// </summary>
    DateTime UploadDateTime { get; }


    /// <summary>
    ///     Returns the stream
    /// </summary>
    Stream Stream { get; }

    /// <summary>
    ///     Returns the file name
    /// </summary>
    string Filename { get; }

    /// <summary>
    ///     Closes the GridFS stream.
    /// </summary>
    void Close();
}