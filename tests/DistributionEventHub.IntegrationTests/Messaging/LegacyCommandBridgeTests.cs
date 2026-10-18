using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     State machine of the <see cref="LegacyCommandBridge" /> (AB#6909) against raw broker entities: it binds the
///     shared queue to the classic exchange only while no classic (exclusive) consumer is alive, and never crashes on
///     a broker refusal.
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class LegacyCommandBridgeTests(RabbitMqFixture rabbitMq) : IAsyncLifetime
{
    private readonly string _legacy = $"bridge-{Guid.NewGuid():N}-cmd";
    private IConnection _admin = null!;
    private IChannel _adminChannel = null!;

    private string Shared => _legacy + "::shared";

    public async ValueTask InitializeAsync()
    {
        _admin = await Connect();
        _adminChannel = await _admin.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _admin.DisposeAsync();
    }

    private Task<IConnection> Connect() => new ConnectionFactory
    {
        HostName = rabbitMq.Host, Port = rabbitMq.Port, UserName = rabbitMq.Username, Password = rabbitMq.Password
    }.CreateConnectionAsync(TestContext.Current.CancellationToken);

    private LegacyCommandBridge NewBridge() => new(new RabbitMqTransportOptions
    {
        Host = rabbitMq.Host, Port = (ushort)rabbitMq.Port, User = rabbitMq.Username, Pass = rabbitMq.Password
    }, _legacy, Shared, TimeSpan.FromMilliseconds(200), NullLogger.Instance);

    private Task DeclareSharedQueue() => _adminChannel.QueueDeclareAsync(Shared, durable: true, exclusive: false,
        autoDelete: false, cancellationToken: TestContext.Current.CancellationToken);

    /// <summary>The "old replica": exclusive, non-durable queue N bound to the non-durable exchange N.</summary>
    private async Task<IConnection> StartClassicConsumer()
    {
        var connection = await Connect();
        var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        await channel.QueueDeclareAsync(_legacy, durable: false, exclusive: true, autoDelete: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await channel.ExchangeDeclareAsync(_legacy, ExchangeType.Fanout, durable: false, autoDelete: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await channel.QueueBindAsync(_legacy, _legacy, "", cancellationToken: TestContext.Current.CancellationToken);
        return connection;
    }

    /// <summary>Publishes like a classic sender; returns the number of messages waiting in the shared queue.</summary>
    private async Task<uint> PublishToClassicExchange()
    {
        await _adminChannel.ExchangeDeclareAsync(_legacy, ExchangeType.Fanout, durable: false, autoDelete: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await _adminChannel.BasicPublishAsync(_legacy, "", new byte[] { 1 },
            cancellationToken: TestContext.Current.CancellationToken);
        await Task.Delay(300, TestContext.Current.CancellationToken);
        return (await _adminChannel.QueueDeclarePassiveAsync(Shared, TestContext.Current.CancellationToken))
            .MessageCount;
    }

    [Fact]
    public async Task SharedQueueDoesNotExistYet_ReportsMissing_AndDoesNotCreateIt()
    {
        await using var bridge = NewBridge();

        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken))
            .Should().Be(LegacyCommandBridgeState.SharedQueueMissing);

        var channel = await _admin.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var probe = async () => await channel.QueueDeclarePassiveAsync(Shared, TestContext.Current.CancellationToken);
        await probe.Should().ThrowAsync<RabbitMQ.Client.Exceptions.OperationInterruptedException>();
    }

    [Fact]
    public async Task NoClassicConsumer_BindsSharedQueueToClassicExchange_AndClassicTrafficArrives()
    {
        await DeclareSharedQueue();
        await using var bridge = NewBridge();

        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Bound);

        (await PublishToClassicExchange()).Should().Be(1u);
    }

    [Fact]
    public async Task ClassicConsumerAlive_DoesNotBind_SoNothingIsDuplicated_AndBindsAfterItIsGone()
    {
        await DeclareSharedQueue();
        await using var bridge = NewBridge();
        var classic = await StartClassicConsumer();

        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken))
            .Should().Be(LegacyCommandBridgeState.LegacyConsumerPresent);
        (await PublishToClassicExchange()).Should().Be(0u, "the classic consumer keeps all classic traffic");

        await classic.DisposeAsync(); // the old replica is gone: exclusive queue and auto-delete exchange vanish
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Bound);
        (await PublishToClassicExchange()).Should().Be(1u, "the shared queue took over");
    }

    [Fact]
    public async Task ClassicConsumerStartsWhileBound_BridgeUnbinds()
    {
        await DeclareSharedQueue();
        await using var bridge = NewBridge();
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Bound);

        await using var classic = await StartClassicConsumer();
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken))
            .Should().Be(LegacyCommandBridgeState.LegacyConsumerPresent);

        (await PublishToClassicExchange()).Should().Be(0u);
    }

    [Fact]
    public async Task ClassicExchangeWithOtherArguments_FaultsWithoutThrowing_AndRecoversWhenItIsRemoved()
    {
        await DeclareSharedQueue();
        await _adminChannel.ExchangeDeclareAsync(_legacy, ExchangeType.Fanout, durable: true, autoDelete: false,
            cancellationToken: TestContext.Current.CancellationToken);
        await using var bridge = NewBridge();

        var act = async () => await bridge.ReconcileAsync(TestContext.Current.CancellationToken);
        (await act.Should().NotThrowAsync()).Subject.Should().Be(LegacyCommandBridgeState.Faulted);

        await _adminChannel.ExchangeDeleteAsync(_legacy, cancellationToken: TestContext.Current.CancellationToken);
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Bound);
    }

    [Fact]
    public async Task BrokerUnreachable_ReconcileFaultsWithoutThrowing()
    {
        await using var bridge = new LegacyCommandBridge(new RabbitMqTransportOptions
        {
            Host = "127.0.0.1", Port = 1, User = "x", Pass = "x"
        }, _legacy, Shared, TimeSpan.FromMilliseconds(200), NullLogger.Instance);

        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Faulted);
    }

    [Fact]
    public async Task BackgroundLoop_FollowsTheClassicConsumer_AndDisposeIsIdempotent()
    {
        await DeclareSharedQueue();
        var bridge = NewBridge();
        bridge.Start();
        await WaitForAsync(bridge, LegacyCommandBridgeState.Bound);

        var classic = await StartClassicConsumer();
        await WaitForAsync(bridge, LegacyCommandBridgeState.LegacyConsumerPresent);

        await classic.DisposeAsync();
        await WaitForAsync(bridge, LegacyCommandBridgeState.Bound);

        await bridge.DisposeAsync();
        await bridge.DisposeAsync();
    }

    private static async Task WaitForAsync(LegacyCommandBridge bridge, LegacyCommandBridgeState expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (bridge.State != expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        bridge.State.Should().Be(expected);
    }
}
