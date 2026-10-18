using System.Collections.Concurrent;
using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     Migration of the classic exclusive command queues to the shared topology (AB#6909): mixed fleets of old
///     (exclusive) and new (shared) replicas on a private RabbitMQ container, with a ledger that proves every command
///     is processed at most once and — outside the documented hand-over window — exactly once.
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class LegacyCommandMigrationTests(RabbitMqFixture rabbitMq) : IAsyncLifetime
{
    private const string CommandName = "migrate::cmd";
    private static readonly TimeSpan ProbeInterval = TimeSpan.FromMilliseconds(300);

    private readonly List<Replica> _replicas = [];
    private readonly MigrationLedger _ledger = new();
    private readonly string _instancePrefix = $"migrate-{Guid.NewGuid():N}";

    private string Prefixed => $"{_instancePrefix}-{CommandName}";
    private string SharedQueue => $"{Prefixed}::shared";

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var replica in _replicas.ToList())
        {
            await StopReplicaAsync(replica);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // replicas

    private sealed record Replica(string Name, ServiceProvider Provider);

    private static readonly CommandConsumerOptions SharedOptions = new()
    {
        MigratesLegacyCommand = true, LegacyProbeInterval = ProbeInterval, PrefetchCount = 1
    };

    private async Task<Replica> StartReplicaAsync(string name, Action<IDistributionEventHubConfiguration> register)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_ledger);
        services.AddSingleton(new ReplicaIdentity(name));
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = _instancePrefix;
            o.BrokerHost = rabbitMq.Host;
            o.BrokerPort = (ushort)rabbitMq.Port;
            o.BrokerUser = rabbitMq.Username;
            o.BrokerPassword = rabbitMq.Password;
        });
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = name;
            config.AutomaticallyStartBusDuringStartup = false;
            register(config);
        });

        var provider = services.BuildServiceProvider();
        var replica = new Replica(name, provider);
        _replicas.Add(replica);
        await provider.GetRequiredService<IEventHubControl>().StartAsync(TestContext.Current.CancellationToken);
        return replica;
    }

    /// <summary>An old replica: the classic <c>AddCommandConsumer(name)</c> (exclusive queue).</summary>
    private Task<Replica> StartOldReplicaAsync(string name) => StartReplicaAsync(name,
        config => config.AddCommandConsumer<MigrationConsumer, MigrationRequest>(CommandName));

    /// <summary>A new replica: <c>AddCommandConsumer(name, options)</c> with the migration switch.</summary>
    private Task<Replica> StartNewReplicaAsync(string name) => StartReplicaAsync(name,
        config => config.AddCommandConsumer<MigrationConsumer, MigrationRequest>(CommandName, SharedOptions));

    /// <summary>A sender that still uses the classic client (<c>AddCommandClient</c>) — never changed.</summary>
    private Task<Replica> StartClassicSenderAsync() => StartReplicaAsync("Sender",
        config => config.AddCommandClient<MigrationRequest>(CommandName, TimeSpan.FromSeconds(10)));

    private async Task StopReplicaAsync(Replica replica)
    {
        if (!_replicas.Remove(replica))
        {
            return;
        }

        try
        {
            await replica.Provider.GetRequiredService<IEventHubControl>().StopAsync();
        }
        catch
        {
            // ignore
        }

        await replica.Provider.DisposeAsync();
    }

    private static async Task<MigrationResponse> SendClassicAsync(Replica sender, string id, int delayMs = 0,
        TimeSpan? timeout = null) =>
        await sender.Provider.GetRequiredService<ICommandClient<MigrationRequest>>()
            .GetResponse<MigrationResponse>(new MigrationRequest { Id = id, DelayMs = delayMs },
                TestContext.Current.CancellationToken, timeout ?? TimeSpan.FromSeconds(10));

    /// <summary>Sends probes until one is answered by a replica that satisfies <paramref name="accepted" />.</summary>
    private async Task WaitUntilServedByAsync(Replica sender, Func<string, bool> accepted, string because)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        var attempt = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var response = await SendClassicAsync(sender, $"probe-{attempt++}", timeout: TimeSpan.FromSeconds(1));
                if (accepted(response.Replica))
                {
                    return;
                }
            }
            catch (DistributionException)
            {
                // nothing is serving yet (hand-over window) — probe again
            }

            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"Timed out waiting until {because}");
    }

    /// <summary>
    ///     Sends probes until one is processed exactly once, by <paramref name="replica" /> (while the bridge and a
    ///     classic replica overlap, a probe is copied to both queues and processed twice).
    /// </summary>
    private async Task WaitUntilExactlyOnceByAsync(Replica sender, string replica, string because)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        for (var attempt = 0; DateTime.UtcNow < deadline; attempt++)
        {
            var id = $"probe-once-{attempt}";
            try
            {
                var response = await SendClassicAsync(sender, id, timeout: TimeSpan.FromSeconds(1));
                await Task.Delay(200, TestContext.Current.CancellationToken);
                if (response.Replica == replica && _ledger.Entries.Count(e => e.Id == id) == 1)
                {
                    return;
                }
            }
            catch (DistributionException)
            {
                // hand-over window — probe again
            }

            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"Timed out waiting until {because}");
    }

    private IEnumerable<LedgerEntry> Real => _ledger.Entries.Where(e => !e.Id.StartsWith("probe-"));

    // ---------------------------------------------------------------------------------------------------------
    // raw broker helpers

    private Task<IConnection> RawConnectionAsync() =>
        new ConnectionFactory
        {
            HostName = rabbitMq.Host, Port = rabbitMq.Port, UserName = rabbitMq.Username,
            Password = rabbitMq.Password
        }.CreateConnectionAsync(TestContext.Current.CancellationToken);

    private static async Task<int?> ReplyCodeOfAsync(IConnection connection, Func<IChannel, Task> action)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            await action(channel);
            return null;
        }
        catch (OperationInterruptedException e)
        {
            return e.ShutdownReason?.ReplyCode;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 1. shared consumers

    [Fact]
    public async Task TwoSharedReplicas_ShareOneQueue_EachCommandIsProcessedOnce_ForClassicAndSendOptionsSenders()
    {
        await StartNewReplicaAsync("Bot-1");
        await StartNewReplicaAsync("Bot-2");
        var sender = await StartClassicSenderAsync();
        await WaitUntilServedByAsync(sender, _ => true, "the bridge serves the classic sender address");

        // classic sender (AddCommandClient → exchange N): reaches the shared queue through the bridge
        var classic = await Task.WhenAll(Enumerable.Range(0, 30)
            .Select(i => SendClassicAsync(sender, $"classic-{i}", delayMs: 40)));
        classic.Select(r => r.Id).Should().BeEquivalentTo(Enumerable.Range(0, 30).Select(i => $"classic-{i}"));

        // new-style sender (send exchange N::send, mandatory): same queue
        var viaSendExchange = await Task.WhenAll(Enumerable.Range(0, 30).Select(i => sender.Provider
            .GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MigrationRequest, MigrationResponse>(CommandName,
                new MigrationRequest { Id = $"send-{i}", DelayMs = 40 },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken)));
        viaSendExchange.Should().HaveCount(30);

        var handled = Real.ToList();
        handled.Should().HaveCount(60);
        handled.GroupBy(e => e.Id).Should().OnlyContain(g => g.Count() == 1, "no command may be processed twice");
        handled.Select(e => e.Replica).Distinct().Should().BeEquivalentTo(["Bot-1", "Bot-2"],
            "both replicas consume the shared queue");
    }

    // ---------------------------------------------------------------------------------------------------------
    // 2. opt-in: a registration without the option is unchanged

    [Fact]
    public async Task ClassicRegistration_WithoutOption_KeepsExclusiveQueue_NoSharedQueueNoSendExchange()
    {
        await StartOldReplicaAsync("Old-1");
        var sender = await StartClassicSenderAsync();
        (await SendClassicAsync(sender, "x")).Replica.Should().Be("Old-1");

        await using var connection = await RawConnectionAsync();
        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(Prefixed)))
            .Should().Be(405, "the classic queue is exclusive and owned by the old replica's connection");
        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(SharedQueue)))
            .Should().Be(404, "a registration without the option declares no shared queue");
        (await ReplyCodeOfAsync(connection, c => c.ExchangeDeclarePassiveAsync(Prefixed + "::send")))
            .Should().Be(404, "a registration without the option declares no send exchange");
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3. mixed fleet: old exclusive replica first, new shared replicas join

    [Fact]
    public async Task MixedFleet_OldReplicaRuns_NewSharedReplicasStartWithoutError_AndStandBy()
    {
        var old = await StartOldReplicaAsync("Old-1");
        var sender = await StartClassicSenderAsync();

        // two new replicas join while the old one holds the exclusive queue: no PRECONDITION_FAILED, no RESOURCE_LOCKED
        await StartNewReplicaAsync("Bot-1");
        await StartNewReplicaAsync("Bot-2");
        await Task.Delay(ProbeInterval * 3, TestContext.Current.CancellationToken);

        var responses = await Task.WhenAll(Enumerable.Range(0, 20).Select(i => SendClassicAsync(sender, $"m-{i}")));

        responses.Should().OnlyContain(r => r.Replica == "Old-1", "classic traffic stays with the old replica");
        Real.GroupBy(e => e.Id).Should().OnlyContain(g => g.Count() == 1);
        Real.Should().HaveCount(20);

        await using var connection = await RawConnectionAsync();
        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(Prefixed))).Should().Be(405);
        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(SharedQueue)))
            .Should().BeNull("the shared queue exists and is consumed by the new replicas");
        old.Name.Should().Be("Old-1");
    }

    // ---------------------------------------------------------------------------------------------------------
    // 4. rolling update: the old replica goes away, the new ones take over

    [Fact]
    public async Task RollingUpdate_OldReplicaStops_NewReplicasTakeOver_NoCommandProcessedTwice()
    {
        var old = await StartOldReplicaAsync("Old-1");
        var sender = await StartClassicSenderAsync();
        await StartNewReplicaAsync("Bot-1");
        await StartNewReplicaAsync("Bot-2");
        await Task.Delay(ProbeInterval * 3, TestContext.Current.CancellationToken);

        // phase A: everything goes to the old replica
        await Task.WhenAll(Enumerable.Range(0, 10).Select(i => SendClassicAsync(sender, $"a-{i}")));
        Real.Should().OnlyContain(e => e.Replica == "Old-1");

        // commands in flight at the moment of the switch: each one is answered once or not at all, never twice
        var inFlight = Enumerable.Range(0, 6)
            .Select(i => SendOrNullAsync(sender, $"flight-{i}", delayMs: 200)).ToList();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        await StopReplicaAsync(old);
        var inFlightResults = await Task.WhenAll(inFlight);
        inFlightResults.Should().OnlyContain(r => r != null && r.Replica == "Old-1",
            "commands already delivered to the old replica are completed before it stops");

        // hand-over: classic senders reach the new replicas as soon as the bridge noticed the old one is gone
        await WaitUntilServedByAsync(sender, r => r is "Bot-1" or "Bot-2", "a new replica serves classic senders");

        // phase B: all commands are answered, spread over both new replicas
        var phaseB = await Task.WhenAll(Enumerable.Range(0, 30)
            .Select(i => SendClassicAsync(sender, $"b-{i}", delayMs: 40)));
        phaseB.Should().OnlyContain(r => r.Replica == "Bot-1" || r.Replica == "Bot-2");
        phaseB.Select(r => r.Replica).Distinct().Should().HaveCount(2);

        Real.GroupBy(e => e.Id).Should().OnlyContain(g => g.Count() == 1, "no command may be processed twice");
        foreach (var answered in inFlightResults.Where(r => r != null))
        {
            Real.Count(e => e.Id == answered!.Id).Should().Be(1);
        }

        Real.Count(e => e.Id.StartsWith("b-")).Should().Be(30);
    }

    private static async Task<MigrationResponse?> SendOrNullAsync(Replica sender, string id, int delayMs)
    {
        try
        {
            return await SendClassicAsync(sender, id, delayMs, TimeSpan.FromSeconds(5));
        }
        catch (DistributionException)
        {
            return null; // lost in the hand-over window: documented, the sender's own timeout/retry covers it
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 5. rollback: a classic replica starts while the shared queue is bound

    [Fact]
    public async Task Rollback_ClassicReplicaStartsWhileSharedQueueServesClassicSenders_BridgeStepsAside()
    {
        await StartNewReplicaAsync("Bot-1");
        var sender = await StartClassicSenderAsync();
        await WaitUntilServedByAsync(sender, r => r == "Bot-1", "the shared replica serves classic senders");

        await StartOldReplicaAsync("Old-1");
        await WaitUntilExactlyOnceByAsync(sender, "Old-1", "the bridge stepped aside for the classic replica");
        _ledger.Clear();

        var responses = await Task.WhenAll(Enumerable.Range(0, 20).Select(i => SendClassicAsync(sender, $"r-{i}")));

        responses.Should().OnlyContain(r => r.Replica == "Old-1");
        Real.GroupBy(e => e.Id).Should().OnlyContain(g => g.Count() == 1, "no duplicate after the bridge stepped aside");
        Real.Should().HaveCount(20);
    }

    // ---------------------------------------------------------------------------------------------------------
    // 6. failure cases: the queue is already declared exclusive

    [Fact]
    public async Task QueueDeclaredExclusiveByForeignConnection_SharedReplicaStartsAndReactsWithoutPreconditionFailed()
    {
        // an "old replica" that is not a MassTransit endpoint at all: raw connection, exclusive queue, bound exchange
        await using var oldConnection = await RawConnectionAsync();
        var oldChannel = await oldConnection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        await oldChannel.QueueDeclareAsync(Prefixed, durable: false, exclusive: true, autoDelete: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await oldChannel.ExchangeDeclareAsync(Prefixed, ExchangeType.Fanout, durable: false, autoDelete: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await oldChannel.QueueBindAsync(Prefixed, Prefixed, "", cancellationToken: TestContext.Current.CancellationToken);

        var act = async () => await StartNewReplicaAsync("Bot-1");
        await act.Should().NotThrowAsync("the shared topology uses other names, so nothing collides");

        await using var connection = await RawConnectionAsync();
        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(SharedQueue))).Should().BeNull();
        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(Prefixed))).Should().Be(405);
    }

    [Fact]
    public async Task Premise_SameNameSharedTopology_ConflictsWithTheClassicQueueAndExchange()
    {
        // This is why a migrated command uses the name "N::shared" instead of reusing "N" (decision of AB#6909).
        await StartOldReplicaAsync("Old-1");
        await using var connection = await RawConnectionAsync();

        (await ReplyCodeOfAsync(connection, c => c.QueueDeclareAsync(Prefixed, durable: true, exclusive: false,
                autoDelete: true)))
            .Should().Be(405, "the exclusive queue of the old replica cannot be declared by another connection");
        (await ReplyCodeOfAsync(connection, c => c.ExchangeDeclareAsync(Prefixed, ExchangeType.Fanout, durable: true,
                autoDelete: true)))
            .Should().Be(406, "the classic endpoint exchange is non-durable; a durable redeclare is PRECONDITION_FAILED");
    }

    // ---------------------------------------------------------------------------------------------------------
    // 7. dynamic registration (IEventHubControl) with the same switch

    [Fact]
    public async Task DynamicRegistration_WithMigrationSwitch_SharesTheQueue_AndServesClassicSenders()
    {
        var hostA = await StartReplicaAsync("Dyn-1", _ => { });
        var hostB = await StartReplicaAsync("Dyn-2", _ => { });
        var sender = await StartClassicSenderAsync();

        await using var handleA = Register(hostA);
        await using var handleB = Register(hostB);
        await handleA.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
        await handleB.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
        await WaitUntilServedByAsync(sender, _ => true, "the bridge serves the classic sender address");

        await Task.WhenAll(Enumerable.Range(0, 30).Select(i => SendClassicAsync(sender, $"d-{i}", delayMs: 40)));

        Real.GroupBy(e => e.Id).Should().OnlyContain(g => g.Count() == 1);
        Real.Should().HaveCount(30);
        Real.Select(e => e.Replica).Distinct().Should().HaveCount(2);

        // disposing the handle stops the endpoint; the other handle keeps serving
        await handleA.DisposeAsync();
        _ledger.Clear();
        await Task.WhenAll(Enumerable.Range(0, 10).Select(i => SendClassicAsync(sender, $"e-{i}")));
        Real.Should().OnlyContain(e => e.Replica == "Dyn-2");

        EndpointHandle Register(Replica host) => host.Provider.GetRequiredService<IEventHubControl>()
            .RegisterCommandConsumer<MigrationRequest>(CommandName, async (message, respond) =>
            {
                _ledger.Add(message.Id, host.Name);
                await Task.Delay(message.DelayMs);
                await respond(new MigrationResponse { Id = message.Id, Replica = host.Name });
            }, SharedOptions);
    }
}

public record MigrationRequest
{
    public string Id { get; init; } = null!;
    public int DelayMs { get; init; }
}

public record MigrationResponse
{
    public string Id { get; init; } = null!;
    public string Replica { get; init; } = null!;
}

public record ReplicaIdentity(string Name);

public record LedgerEntry(string Id, string Replica);

public sealed class MigrationLedger
{
    private readonly ConcurrentQueue<LedgerEntry> _entries = new();

    public IReadOnlyCollection<LedgerEntry> Entries => _entries.ToArray();

    public void Add(string id, string replica) => _entries.Enqueue(new LedgerEntry(id, replica));

    public void Clear() => _entries.Clear();
}

public class MigrationConsumer(MigrationLedger ledger, ReplicaIdentity replica)
    : IDistributedConsumer<MigrationRequest>
{
    public async Task ConsumeAsync(IDistributedContext<MigrationRequest> context)
    {
        ledger.Add(context.Message.Id, replica.Name);
        if (context.Message.DelayMs > 0)
        {
            await Task.Delay(context.Message.DelayMs);
        }

        await context.RespondAsync(new MigrationResponse { Id = context.Message.Id, Replica = replica.Name });
    }
}
