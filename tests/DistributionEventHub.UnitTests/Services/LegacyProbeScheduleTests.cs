using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

/// <summary>
///     Back-off schedule of the legacy command bridge's classic probe (AB#8404): bounded RabbitMQ channel exceptions
///     while bound, full speed in every other state.
/// </summary>
[Trait("Category", "Unit")]
public class LegacyProbeScheduleTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);

    /// <summary>Drives the schedule like the bridge loop: one tick per interval, probe when due.</summary>
    private static List<TimeSpan> ProbeTimes(LegacyProbeSchedule schedule, TimeSpan window,
        Func<TimeSpan, LegacyCommandBridgeState> stateAt)
    {
        var probes = new List<TimeSpan>();
        for (var now = TimeSpan.Zero; now < window; now += schedule.Interval)
        {
            if (!schedule.IsDue(now))
            {
                continue;
            }

            probes.Add(now);
            schedule.Record(stateAt(now), now);
        }

        return probes;
    }

    private static List<TimeSpan> Gaps(IReadOnlyList<TimeSpan> probes) =>
        probes.Zip(probes.Skip(1), (a, b) => b - a).ToList();

    [Fact]
    public void Defaults_CapIsSixtyIntervals_TwoMinutesAtTheDefaultInterval()
    {
        var schedule = new LegacyProbeSchedule(LegacyCommandBridge.DefaultInterval);

        schedule.Interval.Should().Be(TimeSpan.FromSeconds(2));
        schedule.MaxInterval.Should().Be(TimeSpan.FromMinutes(2));
        schedule.IsDue(TimeSpan.Zero).Should().BeTrue("a fresh bridge probes on its first tick");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void NonPositiveInterval_IsRejected(int seconds)
    {
        var act = () => new LegacyProbeSchedule(TimeSpan.FromSeconds(seconds));

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Bound_GapsDoubleFromOneIntervalUpToTheCap()
    {
        var schedule = new LegacyProbeSchedule(Interval);

        var probes = ProbeTimes(schedule, TimeSpan.FromMinutes(10), _ => LegacyCommandBridgeState.Bound);

        Gaps(probes).Take(8).Select(g => g.TotalSeconds).Should()
            .Equal(2, 4, 8, 16, 32, 64, 120, 120);
        Gaps(probes).Should().OnlyContain(g => g <= schedule.MaxInterval);
    }

    [Theory]
    [InlineData(nameof(LegacyCommandBridgeState.LegacyConsumerPresent))]
    [InlineData(nameof(LegacyCommandBridgeState.SharedQueueMissing))]
    [InlineData(nameof(LegacyCommandBridgeState.Faulted))]
    [InlineData(nameof(LegacyCommandBridgeState.Unknown))]
    public void NotBound_ProbesEveryInterval(string stateName)
    {
        var state = Enum.Parse<LegacyCommandBridgeState>(stateName);
        var schedule = new LegacyProbeSchedule(Interval);

        var probes = ProbeTimes(schedule, TimeSpan.FromMinutes(1), _ => state);

        probes.Should().HaveCount(30, "the take-over after the last classic replica stopped must stay within one interval");
        Gaps(probes).Should().OnlyContain(g => g == Interval);
    }

    [Fact]
    public void LeavingBound_DropsTheBackOff_AndTheNextBoundPhaseStartsAtOneIntervalAgain()
    {
        var schedule = new LegacyProbeSchedule(Interval);
        var classicAliveFrom = TimeSpan.FromMinutes(5);
        var classicGoneFrom = TimeSpan.FromMinutes(9);

        var probes = ProbeTimes(schedule, TimeSpan.FromMinutes(10), now =>
            now >= classicAliveFrom && now < classicGoneFrom
                ? LegacyCommandBridgeState.LegacyConsumerPresent
                : LegacyCommandBridgeState.Bound);

        var whileAlive = probes.Where(p => p > classicAliveFrom + schedule.MaxInterval && p < classicGoneFrom).ToList();
        whileAlive.Should().NotBeEmpty();
        Gaps(whileAlive).Should().OnlyContain(g => g == Interval, "a classic consumer is watched at full speed");

        var firstBoundAgain = probes.First(p => p >= classicGoneFrom);
        Gaps(probes.Where(p => p >= firstBoundAgain).ToList()).Take(3).Select(g => g.TotalSeconds).Should()
            .Equal(2, 4, 8);
    }

    [Fact]
    public void ProbeNow_MakesTheNextTickDue_AndRestartsTheBackOff()
    {
        var schedule = new LegacyProbeSchedule(Interval);
        ProbeTimes(schedule, TimeSpan.FromMinutes(10), _ => LegacyCommandBridgeState.Bound);
        schedule.CurrentGap.Should().Be(schedule.MaxInterval);

        schedule.ProbeNow();

        schedule.IsDue(TimeSpan.FromMinutes(10)).Should().BeTrue();
        schedule.CurrentGap.Should().Be(TimeSpan.Zero);
        schedule.Record(LegacyCommandBridgeState.Bound, TimeSpan.FromMinutes(10));
        schedule.CurrentGap.Should().Be(Interval);
    }

    [Fact]
    public void ReactionBound_AClassicConsumerAppearingWhileBound_IsSeenWithinCapPlusOneInterval()
    {
        var schedule = new LegacyProbeSchedule(Interval);
        var appearsAt = TimeSpan.FromMinutes(30) + TimeSpan.FromSeconds(1); // just after a probe, worst case

        var probes = ProbeTimes(schedule, TimeSpan.FromMinutes(40), now =>
            now >= appearsAt ? LegacyCommandBridgeState.LegacyConsumerPresent : LegacyCommandBridgeState.Bound);

        var detectedAt = probes.First(p => p >= appearsAt);
        (detectedAt - appearsAt).Should().BeLessThanOrEqualTo(schedule.MaxInterval + schedule.Interval);
    }

    [Fact]
    public void TickJitter_HalfAnIntervalTolerance_KeepsTheGapsOnTheTickGrid()
    {
        var schedule = new LegacyProbeSchedule(Interval);
        schedule.Record(LegacyCommandBridgeState.Bound, TimeSpan.FromMilliseconds(5)); // probe started 5 ms late

        schedule.IsDue(TimeSpan.FromMilliseconds(1999)).Should().BeTrue("a slightly early tick must not cost a whole interval");
        schedule.IsDue(TimeSpan.FromMilliseconds(900)).Should().BeFalse();
    }

    [Fact]
    public void OneDayWithoutClassicQueue_ProbesLessThanFivePercentOfTheFormerRate()
    {
        var schedule = new LegacyProbeSchedule(LegacyCommandBridge.DefaultInterval);
        var day = TimeSpan.FromDays(1);
        var formerProbes = (int)(day / LegacyCommandBridge.DefaultInterval); // 43,200 channel exceptions per bridge

        var probes = ProbeTimes(schedule, day, _ => LegacyCommandBridgeState.Bound);

        probes.Count.Should().BeLessThan(formerProbes / 20, "AB#8404 asks for at least 95 % fewer not_found errors");
        probes.Count.Should().BeLessThanOrEqualTo(730); // ~ 24 h / 2 min + the ramp-up
    }
}
