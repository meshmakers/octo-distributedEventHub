using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

public class PendingMessageCoalescerTests
{
    private static Func<Task<uint?>> Pending(uint? count) => () => Task.FromResult(count);

    [Fact]
    public async Task NothingWaiting_HandlesMessage()
    {
        var sut = new PendingMessageCoalescer();

        var decision = await sut.DecideAsync(Pending(0));

        decision.Should().NotBeNull();
        decision!.CoalescedMessageCount.Should().Be(0);
    }

    [Fact]
    public async Task NewerMessagesWaiting_SkipsMessage()
    {
        var sut = new PendingMessageCoalescer();

        var decision = await sut.DecideAsync(Pending(3));

        decision.Should().BeNull();
        sut.PendingCoalescedCount.Should().Be(1);
    }

    [Fact]
    public async Task BacklogOfTwenty_HandlesOnlyTheLastAndReportsNineteenCoalesced()
    {
        var sut = new PendingMessageCoalescer();
        var handled = new List<RoutedEventDeliveryContext>();

        // With prefetch 1 the broker reports the messages behind the current delivery: 19, 18, ... 0.
        for (uint behind = 19; ; behind--)
        {
            var decision = await sut.DecideAsync(Pending(behind));
            if (decision != null)
            {
                handled.Add(decision);
            }

            if (behind == 0)
            {
                break;
            }
        }

        handled.Should().ContainSingle().Which.CoalescedMessageCount.Should().Be(19);
        sut.PendingCoalescedCount.Should().Be(0);
    }

    [Fact]
    public async Task CoalescedCount_ResetsAfterHandledMessage()
    {
        var sut = new PendingMessageCoalescer();

        await sut.DecideAsync(Pending(1));
        (await sut.DecideAsync(Pending(0)))!.CoalescedMessageCount.Should().Be(1);

        (await sut.DecideAsync(Pending(0)))!.CoalescedMessageCount.Should().Be(0);
    }

    [Fact]
    public async Task UnknownPendingCount_FailsOpen()
    {
        var sut = new PendingMessageCoalescer();

        var decision = await sut.DecideAsync(Pending(null));

        decision.Should().NotBeNull();
    }

    [Fact]
    public async Task ProviderThrows_FailsOpen()
    {
        var sut = new PendingMessageCoalescer();

        var decision = await sut.DecideAsync(() => throw new InvalidOperationException("broker gone"));

        decision.Should().NotBeNull();
    }
}
