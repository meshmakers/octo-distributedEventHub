namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     Decides when the <see cref="LegacyCommandBridge" /> probes the classic queue again (AB#8404).
/// </summary>
/// <remarks>
///     <para>
///         The probe is a passive <c>queue.declare</c> of the classic queue. When the queue does not exist, RabbitMQ
///         answers 404, closes the channel and writes an error line ("operation queue.declare caused a channel
///         exception not_found") to its log. Once every classic replica is gone that is the steady state, so probing
///         at the base interval wrote one error line per migrated command, replica and interval (about 600,000 lines
///         per cluster and day with 7 migrated commands at 2 s).
///     </para>
///     <para>
///         Only the state <see cref="LegacyCommandBridgeState.Bound" /> backs off: after the first <c>Bound</c> result
///         the next probe follows after one interval, then the gap doubles up to
///         <see cref="MaxInterval" /> (= <see cref="MaxIntervalFactor" /> intervals, 2 minutes at the default 2 s).
///         Every other result (a classic consumer alive, shared queue missing, faulted) probes at the base interval,
///         so the take-over after the last old replica stopped is as fast as before. <see cref="ProbeNow" /> returns
///         to the base interval at once (the bridge calls it when the consumer count of the shared queue changed).
///     </para>
///     <para>
///         The schedule is driven by a timer that ticks every <see cref="Interval" />; a probe due within half an
///         interval of a tick runs on that tick. The class is not thread-safe; the bridge loop is its only caller.
///     </para>
/// </remarks>
internal sealed class LegacyProbeSchedule
{
    /// <summary>The back-off is capped at this many base intervals.</summary>
    internal const int MaxIntervalFactor = 60;

    private TimeSpan _nextGap;

    /// <param name="interval">Base interval (<c>LegacyProbeInterval</c>), must be positive</param>
    public LegacyProbeSchedule(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        Interval = interval;
        MaxInterval = interval * MaxIntervalFactor;
        _nextGap = interval;
    }

    /// <summary>Base interval between two probes while the bridge is not <c>Bound</c>.</summary>
    public TimeSpan Interval { get; }

    /// <summary>Largest gap between two probes while the bridge is <c>Bound</c>.</summary>
    public TimeSpan MaxInterval { get; }

    /// <summary>
    ///     Gap that was applied after the last probe (<see cref="TimeSpan.Zero" /> when the next probe runs on the next
    ///     tick).
    /// </summary>
    public TimeSpan CurrentGap { get; private set; }

    /// <summary>Time of the next probe on the caller's monotonic clock; <c>null</c> = on the next tick.</summary>
    public TimeSpan? NextProbeAt { get; private set; }

    /// <summary>Whether the probe is due at <paramref name="now" /> (with half an interval of tick tolerance).</summary>
    public bool IsDue(TimeSpan now) => NextProbeAt is not { } at || now >= at - Interval / 2;

    /// <summary>Records the result of a probe that started at <paramref name="startedAt" />.</summary>
    public void Record(LegacyCommandBridgeState state, TimeSpan startedAt)
    {
        if (state != LegacyCommandBridgeState.Bound)
        {
            ProbeNow();
            return;
        }

        CurrentGap = _nextGap;
        NextProbeAt = startedAt + _nextGap;
        _nextGap = _nextGap * 2 > MaxInterval ? MaxInterval : _nextGap * 2;
    }

    /// <summary>Drops the back-off: the next tick probes, and the next <c>Bound</c> result starts again at one interval.</summary>
    public void ProbeNow()
    {
        _nextGap = Interval;
        CurrentGap = TimeSpan.Zero;
        NextProbeAt = null;
    }
}
