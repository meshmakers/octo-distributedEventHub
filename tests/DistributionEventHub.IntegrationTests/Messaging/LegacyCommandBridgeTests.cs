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

    /// <summary>
    ///     AB#8623: without a configured broker user (the local default) the bridge connects with the same defaults as
    ///     MassTransit (<c>guest</c>, the user of the test broker) instead of failing with
    ///     <c>Value cannot be null (Parameter 'userName')</c> on every reconcile.
    /// </summary>
    [Fact]
    public async Task NoBrokerUserConfigured_ConnectsWithTheMassTransitDefaults_AndBinds()
    {
        await DeclareSharedQueue();
        await using var bridge = new LegacyCommandBridge(new RabbitMqTransportOptions
        {
            Host = rabbitMq.Host, Port = (ushort)rabbitMq.Port, User = null, Pass = null
        }, _legacy, Shared, TimeSpan.FromMilliseconds(200), NullLogger.Instance);

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

    // ---------------------------------------------------------------------------------------------------------
    // AB#8404: back-off of the classic probe while bound

    private const int FastIntervalMs = 100; // cap = 60 intervals = 6 s

    private LegacyCommandBridge NewFastBridge() => NewFastBridge(rabbitMq.Username, rabbitMq.Password);

    private LegacyCommandBridge NewFastBridge(string user, string password) => new(new RabbitMqTransportOptions
    {
        Host = rabbitMq.Host, Port = (ushort)rabbitMq.Port, User = user, Pass = password
    }, _legacy, Shared, TimeSpan.FromMilliseconds(FastIntervalMs), NullLogger.Instance);

    /// <summary>A bridge whose broker user may publish to the default exchange (the core services' user).</summary>
    private LegacyCommandBridge NewCoreBridge() => NewFastBridge(rabbitMq.CoreUsername, rabbitMq.CorePassword);

    /// <summary>RabbitMQ's error lines "operation queue.declare caused a channel exception not_found: no queue 'N'".</summary>
    private async Task<int> BrokerNotFoundErrorsForClassicQueueAsync()
    {
        var log = await rabbitMq.GetBrokerLogAsync();
        var needle = $"no queue '{_legacy}'";
        var count = 0;
        for (var i = log.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = log.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static async Task WaitForCapAsync(LegacyCommandBridge bridge)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (bridge.Schedule.CurrentGap < bridge.Schedule.MaxInterval && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        bridge.Schedule.CurrentGap.Should().Be(bridge.Schedule.MaxInterval, "the bridge should have backed off fully");
    }

    /// <summary>Polls the broker log until it shows at least <paramref name="expected" /> errors (or 10 s passed).</summary>
    private async Task<int> WaitForBrokerErrorsAsync(int expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        var errors = await BrokerNotFoundErrorsForClassicQueueAsync();
        while (errors < expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200, TestContext.Current.CancellationToken);
            errors = await BrokerNotFoundErrorsForClassicQueueAsync();
        }

        return errors;
    }

    // ---------------------------------------------------------------------------------------------------------
    // AB#8408: no channel exceptions in the steady state, independent of the number of replicas

    [Fact]
    public async Task BackgroundLoop_NoClassicQueue_TwoReplicas_NoRecurringChannelExceptions()
    {
        await DeclareSharedQueue();
        var window = TimeSpan.FromSeconds(12);
        await using var replica1 = NewCoreBridge();
        await using var replica2 = NewCoreBridge();

        replica1.Start();
        replica2.Start();
        await WaitForAsync(replica1, LegacyCommandBridgeState.Bound);
        await WaitForAsync(replica2, LegacyCommandBridgeState.Bound);
        var errorsAfterStart = await WaitForBrokerErrorsAsync(2);

        await Task.Delay(window, TestContext.Current.CancellationToken);
        await replica1.DisposeAsync();
        await replica2.DisposeAsync();

        // The broker's own log is the metric of AB#8408: one not_found error per bridge start, none afterwards.
        errorsAfterStart.Should().Be(2, "each bridge's first probe is a passive declare");
        await Task.Delay(1000, TestContext.Current.CancellationToken); // let the broker flush its log
        (await BrokerNotFoundErrorsForClassicQueueAsync()).Should().Be(errorsAfterStart,
            "while bound the bridges probe with a mandatory publish, which RabbitMQ does not log as an error");

        foreach (var replica in new[] { replica1, replica2 })
        {
            replica.State.Should().Be(LegacyCommandBridgeState.Bound);
            replica.DeclareProbeCount.Should().Be(1);
            replica.PublishProbeCount.Should().BeGreaterThanOrEqualTo(5, "the classic queue is still checked while bound");
            replica.PublishProbeUnavailable.Should().BeFalse();
        }
    }

    [Fact]
    public async Task Bound_ClassicConsumerAppears_PublishProbeSeesIt_WithoutChannelException_AndLeavesNoMessage()
    {
        await DeclareSharedQueue();
        await using var bridge = NewCoreBridge();
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Bound);

        await using var classic = await StartClassicConsumer();
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken))
            .Should().Be(LegacyCommandBridgeState.LegacyConsumerPresent);

        bridge.DeclareProbeCount.Should().Be(1, "only the first pass (state Unknown) used the passive declare");
        bridge.PublishProbeCount.Should().Be(1);

        // the probe expires in the classic queue (no consumer attached; RabbitMQ drops it from the head of the queue
        // shortly after), nothing is left behind
        var owner = await classic.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var deadline = DateTime.UtcNow.AddSeconds(5);
        uint waiting;
        while ((waiting = (await owner.QueueDeclarePassiveAsync(_legacy, TestContext.Current.CancellationToken))
                   .MessageCount) > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        waiting.Should().Be(0u, "the probe message carries expiration 0");

        (await PublishToClassicExchange()).Should().Be(0u, "the bridge stepped aside");
    }

    [Fact]
    public async Task SharedQueueMissing_DoesNotProbeTheClassicQueue()
    {
        await using var bridge = NewCoreBridge();

        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken))
            .Should().Be(LegacyCommandBridgeState.SharedQueueMissing);
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken))
            .Should().Be(LegacyCommandBridgeState.SharedQueueMissing);

        bridge.ClassicProbeCount.Should().Be(0, "nothing to bind while the shared queue is missing, so no channel exception");
    }

    [Fact]
    public async Task UserWithoutDefaultExchangeWrite_FallsBackToPassiveDeclare_AndStillStepsAside()
    {
        await DeclareSharedQueue();
        // the restricted adapter user (AB#7338) may not write to amq.default
        await using var bridge = NewFastBridge(rabbitMq.AdapterUsername, rabbitMq.AdapterPassword);

        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Bound);
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken)).Should().Be(LegacyCommandBridgeState.Bound,
            "a refused publish probe falls back to the passive declare in the same pass");
        bridge.PublishProbeUnavailable.Should().BeTrue();
        bridge.PublishProbeCount.Should().Be(1, "the publish probe is not tried again after the refusal");
        bridge.DeclareProbeCount.Should().Be(2);

        await using var classic = await StartClassicConsumer();
        (await bridge.ReconcileAsync(TestContext.Current.CancellationToken))
            .Should().Be(LegacyCommandBridgeState.LegacyConsumerPresent);
        bridge.PublishProbeCount.Should().Be(1);
    }

    [Fact]
    public async Task BackgroundLoop_ClassicConsumerAppearsAfterFullBackOff_StepsAsideWithinCapPlusOneInterval()
    {
        await DeclareSharedQueue();
        await using var bridge = NewFastBridge();
        bridge.Start();
        await WaitForAsync(bridge, LegacyCommandBridgeState.Bound);
        await WaitForCapAsync(bridge);

        // a classic consumer appears, the number of shared consumers does not change: only the backed-off probe sees it
        var appeared = DateTime.UtcNow;
        await using var classic = await StartClassicConsumer();
        await WaitForAsync(bridge, LegacyCommandBridgeState.LegacyConsumerPresent);
        var reaction = DateTime.UtcNow - appeared;

        var bound = bridge.Schedule.MaxInterval + bridge.Schedule.Interval;
        reaction.Should().BeLessThanOrEqualTo(bound + TimeSpan.FromSeconds(1), "documented reaction bound (+1 s slack)");
        (await PublishToClassicExchange()).Should().Be(0u, "after stepping aside nothing is copied to the shared queue");

        // while the classic consumer is alive the bridge probes at the base interval again
        bridge.Schedule.CurrentGap.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task BackgroundLoop_SharedConsumerCountChanges_ProbesAtOnceDespiteTheBackOff()
    {
        await DeclareSharedQueue();
        await using var bridge = NewFastBridge();
        bridge.Start();
        await WaitForAsync(bridge, LegacyCommandBridgeState.Bound);
        await WaitForCapAsync(bridge);
        var probesBefore = bridge.ClassicProbeCount;

        // rollback shape: an old replica starts and a shared replica goes away (here: one comes, the count changes)
        await using var classic = await StartClassicConsumer();
        await using var sharedReplica = await Connect();
        var sharedChannel = await sharedReplica.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var appeared = DateTime.UtcNow;
        await sharedChannel.BasicConsumeAsync(Shared, autoAck: true,
            new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(sharedChannel),
            TestContext.Current.CancellationToken);

        await WaitForAsync(bridge, LegacyCommandBridgeState.LegacyConsumerPresent);
        (DateTime.UtcNow - appeared).Should().BeLessThan(bridge.Schedule.MaxInterval / 2,
            "a changed consumer count drops the back-off on the next tick");
        bridge.ClassicProbeCount.Should().BeGreaterThan(probesBefore);
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
