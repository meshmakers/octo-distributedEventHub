namespace Meshmakers.Octo.Common.DistributionEventHub.Payloads;

/// <summary>
///     Represents a cache stream
/// </summary>
public record CacheStream
{
    /// <summary>
    ///     The content type of the stream
    /// </summary>
    public string ContentType { get; init; } = string.Empty;

    /// <summary>
    ///     The stream as byte array
    /// </summary>
    public Stream Stream { get; init; } = null!;
    
    /// <summary>
    /// The original file name
    /// </summary>
    public string FileName { get; init; } = string.Empty;
}