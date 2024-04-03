namespace Meshmakers.Octo.Common.DistributionEventHub.Payloads;

/// <summary>
///     Represents a cache stream
/// </summary>
public record CacheStream
{
    /// <summary>
    ///     The content type of the stream
    /// </summary>
    public string ContentType
    {
        get;
#if !NETSTANDARD2_0
        init;
#else
        set;
#endif
    } = string.Empty;

    /// <summary>
    ///     The stream as byte array
    /// </summary>
    public Stream Stream
    {
        get;
#if !NETSTANDARD2_0
        init;
#else
        set;
#endif
    } = null!;

    /// <summary>
    ///     The original file name
    /// </summary>
    public string FileName
    {
        get;
#if !NETSTANDARD2_0
        init;
#else
        set;
#endif
    } = string.Empty;
}