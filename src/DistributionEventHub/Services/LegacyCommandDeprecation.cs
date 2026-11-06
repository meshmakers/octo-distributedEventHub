using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Deprecation notice of the classic exclusive command consumer (AB#6909). A compiler-level
///     <c>[Obsolete]</c> is deliberately not used: every consumer repository builds with warnings as errors, and
///     consumers that are exclusive by design (one instance owns the address) must keep compiling. Those declare it
///     with <see cref="CommandConsumerOptions.ExclusiveByDesign" /> and are not warned about (AB#8363).
/// </summary>
internal static class LegacyCommandDeprecation
{
    private static readonly ConcurrentDictionary<string, byte> Warned = new();

    /// <summary>
    ///     Logs one warning per command name and process.
    /// </summary>
    internal static void WarnOnce(ILogger logger, string prefixedCommandName)
    {
        if (!Warned.TryAdd(prefixedCommandName, 0))
        {
            return;
        }

        logger.LogWarning(
            "Command consumer '{Command}' uses the classic EXCLUSIVE topology: only one process at a time consumes " +
            "it, further replicas wait for the owner's connection to end. Deprecated (AB#6909): register it shared " +
            "with CommandConsumerOptions {{ MigratesLegacyCommand = true }}; if exactly one instance must own the " +
            "address, declare it with CommandConsumerOptions.ExclusiveByDesign(reason) (AB#8363)",
            prefixedCommandName);
    }

    /// <summary>
    ///     Logs one warning for all classic <c>AddCommandConsumer</c> registrations of a process.
    /// </summary>
    internal static void WarnOnce(ILogger logger, IReadOnlyCollection<string> prefixedCommandNames)
    {
        var names = prefixedCommandNames.Where(n => Warned.TryAdd(n, 0)).OrderBy(n => n).ToList();
        if (names.Count == 0)
        {
            return;
        }

        logger.LogWarning(
            "{Count} command consumer(s) use the classic EXCLUSIVE topology: {Commands}. Only one process at a " +
            "time consumes each of them, further replicas wait for the owner's connection to end. Deprecated " +
            "(AB#6909): register them with AddCommandConsumer(name, new CommandConsumerOptions {{ " +
            "MigratesLegacyCommand = true }}); if exactly one instance must own the address, declare it with " +
            "CommandConsumerOptions.ExclusiveByDesign(reason) (AB#8363)",
            names.Count, string.Join(", ", names));
    }

    /// <summary>Forgets what was warned about (tests).</summary>
    internal static void ResetForTests() => Warned.Clear();
}
