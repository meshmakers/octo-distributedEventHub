using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

public class CommandSendOptionsUnitTests
{
    [Fact]
    public void IsMessageReturned_DirectMessageReturnedException_True()
    {
        DistributionEventHubService.IsMessageReturned(new MessageReturnedException("returned")).Should().BeTrue();
    }

    [Fact]
    public void IsMessageReturned_WrappedInRequestException_True()
    {
        var wrapped = new RequestException("request failed", new MessageReturnedException("returned"));
        DistributionEventHubService.IsMessageReturned(wrapped).Should().BeTrue();
    }

    [Fact]
    public void IsMessageReturned_InsideAggregateException_True()
    {
        var aggregate = new AggregateException(new InvalidOperationException("x"),
            new MessageReturnedException("returned"));
        DistributionEventHubService.IsMessageReturned(aggregate).Should().BeTrue();
    }

    [Fact]
    public void IsMessageReturned_OtherException_False()
    {
        DistributionEventHubService.IsMessageReturned(new InvalidOperationException("x", new TimeoutException()))
            .Should().BeFalse();
    }

    [Fact]
    public void SharedCommandSendExchangeName_AppendsSendSuffix()
    {
        CacheCommon.GetSharedCommandSendExchangeName("prod-octo::method::a::b::c-1")
            .Should().Be("prod-octo::method::a::b::c-1::send");
    }

    [Fact]
    public void CommandConsumerOptions_Defaults()
    {
        var options = new CommandConsumerOptions();
        options.ConfigureConsumeTopology.Should().BeFalse();
        options.PrefetchCount.Should().BeNull();
        options.ConcurrentMessageLimit.Should().BeNull();
    }

    [Fact]
    public async Task GetCommandResponseAsync_NullOptions_Throws()
    {
        var address = new Mock<IBroadcastServiceAddress>();
        address.Setup(a => a.InstancePrefix).Returns("test");
        var sut = new DistributionEventHubService(new Mock<IBus>().Object, address.Object);

        var act = () => sut.GetCommandResponseAsync<object, object>("cmd", new object(), (CommandSendOptions)null!,
            TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Theory]
    [InlineData(false, 0, 0, false)]
    [InlineData(true, 0, 0, true)]
    [InlineData(false, 1, 0, true)]
    [InlineData(false, 0, 1, true)]
    public void CommandDeliveryInfo_IsPossibleDuplicate(bool redelivered, int retry, int redeliveryCount, bool expected)
    {
        new CommandDeliveryInfo(redelivered, retry, redeliveryCount, Guid.NewGuid()).IsPossibleDuplicate
            .Should().Be(expected);
    }

    [Fact]
    public void Options_Defaults_PersistentTrue_SuppressFaultsFalse()
    {
        new CommandSendOptions().Persistent.Should().BeTrue();
        new CommandConsumerOptions().SuppressFaults.Should().BeFalse();
    }
}
