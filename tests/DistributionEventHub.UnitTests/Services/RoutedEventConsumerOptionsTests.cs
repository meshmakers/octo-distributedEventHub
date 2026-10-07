using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

public class RoutedEventConsumerOptionsTests
{
    [Fact]
    public void Default_KeepsTransportDefaults()
    {
        var options = RoutedEventConsumerOptions.Default;

        options.EffectivePrefetchCount.Should().BeNull();
        options.EffectiveConcurrentMessageLimit.Should().BeNull();
        options.CoalescePendingMessages.Should().BeFalse();
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void LatestOnly_IsOneAtATimeAndCoalesces()
    {
        var options = RoutedEventConsumerOptions.LatestOnly;

        options.EffectivePrefetchCount.Should().Be(1);
        options.EffectiveConcurrentMessageLimit.Should().Be(1);
        options.CoalescePendingMessages.Should().BeTrue();
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Coalescing_WithoutExplicitLimits_DefaultsToOne()
    {
        var options = new RoutedEventConsumerOptions { CoalescePendingMessages = true };

        options.EffectivePrefetchCount.Should().Be(1);
        options.EffectiveConcurrentMessageLimit.Should().Be(1);
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Theory]
    [InlineData(2, null)]
    [InlineData(null, 2)]
    [InlineData(16, 16)]
    public void Coalescing_WithLargerWindow_IsRejected(int? prefetch, int? concurrency)
    {
        var options = new RoutedEventConsumerOptions
        {
            CoalescePendingMessages = true,
            PrefetchCount = prefetch,
            ConcurrentMessageLimit = concurrency
        };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>()
            .WithParameterName(nameof(RoutedEventConsumerOptions.CoalescePendingMessages));
    }

    [Theory]
    [InlineData(0, null)]
    [InlineData(null, 0)]
    [InlineData(-1, 1)]
    public void NonPositiveLimits_AreRejected(int? prefetch, int? concurrency)
    {
        var options = new RoutedEventConsumerOptions
        {
            PrefetchCount = prefetch,
            ConcurrentMessageLimit = concurrency
        };

        options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>();
    }

    [Fact]
    public void ConcurrencyLimitWithoutCoalescing_IsAccepted()
    {
        var options = new RoutedEventConsumerOptions { ConcurrentMessageLimit = 1 };

        options.EffectivePrefetchCount.Should().BeNull();
        options.EffectiveConcurrentMessageLimit.Should().Be(1);
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }
}
