using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

public class OctoRecurringScheduleTests
{
    [Fact]
    public void TimeZoneId_IsTheRequestedZone_WhenOneIsGiven()
    {
        var options = new RecurringSchedulingOptions("0 0 9 * * ?", "schedule-1", "group-1", "Daily summary")
        {
            TimeZoneId = "Europe/Vienna"
        };

        new OctoRecurringSchedule(options).TimeZoneId.Should().Be("Europe/Vienna");
    }

    [Fact]
    public void TimeZoneId_IsTheLocalZone_WhenNoneIsGiven()
    {
        var options = new RecurringSchedulingOptions("0 0 9 * * ?", "schedule-1", "group-1", "Daily summary");

        new OctoRecurringSchedule(options).TimeZoneId.Should().Be(TimeZoneInfo.Local.Id);
    }

    [Fact]
    public void TimeZoneId_IsTheLocalZone_WhenOnlyWhitespaceIsGiven()
    {
        var options = new RecurringSchedulingOptions("0 0 9 * * ?", "schedule-1", "group-1", "Daily summary")
        {
            TimeZoneId = " "
        };

        new OctoRecurringSchedule(options).TimeZoneId.Should().Be(TimeZoneInfo.Local.Id);
    }

    [Fact]
    public void TimeZoneId_AcceptsAWindowsId()
    {
        var options = new RecurringSchedulingOptions("0 0 9 * * ?", "schedule-1", "group-1", "Daily summary")
        {
            TimeZoneId = "W. Europe Standard Time"
        };

        new OctoRecurringSchedule(options).TimeZoneId.Should().Be("W. Europe Standard Time");
    }

    [Fact]
    public void AnUnknownTimeZone_IsRefusedWhereTheScheduleIsCreated()
    {
        var options = new RecurringSchedulingOptions("0 0 9 * * ?", "schedule-1", "group-1", "Daily summary")
        {
            TimeZoneId = "Europe/Viena"
        };

        var create = () => new OctoRecurringSchedule(options);

        create.Should().Throw<ArgumentException>()
            .WithMessage("*'Europe/Viena'*'schedule-1'*");
    }
}
