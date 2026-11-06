using MassTransit;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Runs the <see cref="LegacyCommandBridge" />s of all DI-registered, migrating command consumers for as long as
///     the bus runs (AB#6909). One observer owns every bridge of a configuration: MassTransit's <c>AddBusObserver</c>
///     keeps only one registration per observer type, so one observer per command would serve only the first
///     migrated command (AB#8368).
/// </summary>
internal sealed class LegacyCommandBridgeObserver(
    IReadOnlyCollection<Func<LegacyCommandBridge>> bridgeFactories) : IBusObserver
{
    private List<LegacyCommandBridge>? _bridges;

    /// <summary>The number of bridges this observer starts (one per migrated command).</summary>
    internal int BridgeCount => bridgeFactories.Count;

    public void PostCreate(IBus bus)
    {
    }

    public void CreateFaulted(Exception exception)
    {
    }

    public Task PreStart(IBus bus) => Task.CompletedTask;

    public Task PostStart(IBus bus, Task<BusReady> busReady)
    {
        // Assigned first so that PreStop disposes the bridges already started if a later factory throws.
        var bridges = new List<LegacyCommandBridge>(bridgeFactories.Count);
        _bridges = bridges;
        foreach (var factory in bridgeFactories)
        {
            var bridge = factory();
            bridges.Add(bridge);
            bridge.Start();
        }

        return Task.CompletedTask;
    }

    public Task StartFaulted(IBus bus, Exception exception) => Task.CompletedTask;

    public async Task PreStop(IBus bus)
    {
        var bridges = _bridges;
        _bridges = null;
        if (bridges == null)
        {
            return;
        }

        foreach (var bridge in bridges)
        {
            await bridge.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Task PostStop(IBus bus) => Task.CompletedTask;

    public Task StopFaulted(IBus bus, Exception exception) => Task.CompletedTask;
}

/// <summary>
///     Logs the deprecation notice for the classic exclusive DI command consumers once the bus is created (AB#6909).
/// </summary>
internal sealed class LegacyCommandDeprecationObserver(ILogger logger, IReadOnlyCollection<string> commandNames)
    : IBusObserver
{
    public void PostCreate(IBus bus)
    {
    }

    public void CreateFaulted(Exception exception)
    {
    }

    public Task PreStart(IBus bus) => Task.CompletedTask;

    public Task PostStart(IBus bus, Task<BusReady> busReady)
    {
        LegacyCommandDeprecation.WarnOnce(logger, commandNames);
        return Task.CompletedTask;
    }

    public Task StartFaulted(IBus bus, Exception exception) => Task.CompletedTask;

    public Task PreStop(IBus bus) => Task.CompletedTask;

    public Task PostStop(IBus bus) => Task.CompletedTask;

    public Task StopFaulted(IBus bus, Exception exception) => Task.CompletedTask;
}
