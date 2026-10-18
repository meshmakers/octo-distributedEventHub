using MassTransit;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Runs the <see cref="LegacyCommandBridge" /> of a DI-registered, migrating command consumer for as long as
///     the bus runs (AB#6909).
/// </summary>
internal sealed class LegacyCommandBridgeObserver(
    Func<LegacyCommandBridge> bridgeFactory) : IBusObserver
{
    private LegacyCommandBridge? _bridge;

    public void PostCreate(IBus bus)
    {
    }

    public void CreateFaulted(Exception exception)
    {
    }

    public Task PreStart(IBus bus) => Task.CompletedTask;

    public Task PostStart(IBus bus, Task<BusReady> busReady)
    {
        _bridge = bridgeFactory();
        _bridge.Start();
        return Task.CompletedTask;
    }

    public Task StartFaulted(IBus bus, Exception exception) => Task.CompletedTask;

    public async Task PreStop(IBus bus)
    {
        var bridge = _bridge;
        _bridge = null;
        if (bridge != null)
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
