namespace Meshmakers.Octo.Common.DistributionEventHub;

/// <summary>
///     Common definitions of cache
/// </summary>
public static class CacheCommon
{
    internal const string ServiceEndpointPattern = "octo::service::{0}";

    /// <summary>
    /// Constant string that defines the content type
    /// </summary>
    public const string ContentType = "contentType";
    
    /// <summary>
    /// Constant string that defines the expiry date time
    /// </summary>
    public const string ExpiryDateTime = "expiryDateTime";

    /// <summary>
    /// Gets the instance prefix for a given instance prefix string
    /// </summary>
    /// <param name="instancePrefix">The instance prefix to apply</param>
    /// <returns>The formatted instance prefix</returns>
    internal static string GetInstancePrefix(string instancePrefix)
    {
        return $"{instancePrefix.ToLower()}-";
    }
    
    /// <summary>
    /// Applies instance prefix to a name if the prefix is not null or empty
    /// </summary>
    /// <param name="instancePrefix">The instance prefix to apply</param>
    /// <param name="name">The original name</param>
    /// <returns>The prefixed name or original name if no prefix</returns>
    internal static string ApplyInstancePrefix(string instancePrefix, string name)
    {
        return GetInstancePrefix(instancePrefix) + name;
    }
    
    /// <summary>
    /// Applies instance prefix to a URI if prefix is not null or empty
    /// </summary>
    /// <param name="instancePrefix">The instance prefix to apply</param>
    /// <param name="uri">The original URI</param>
    /// <returns>The prefixed URI</returns>
    internal static Uri ApplyInstancePrefixToUri(string instancePrefix, Uri uri)
    {
        if (string.IsNullOrWhiteSpace(instancePrefix))
        {
            return uri;
        }

        var uriString = uri.ToString();
        
        // Handle queue: and exchange: schemes
        if (uriString.StartsWith("queue:"))
        {
            var queueName = uriString.Substring(6); // Remove "queue:"
            return new Uri($"queue:{GetInstancePrefix(instancePrefix)}{queueName}");
        }

        if (uriString.StartsWith("exchange:"))
        {
            var exchangePart = uriString.Substring(9); // Remove "exchange:"
            return new Uri($"exchange:{GetInstancePrefix(instancePrefix)}{exchangePart}");
        }

        // For other URI formats, return as-is
        return uri;
    }
}