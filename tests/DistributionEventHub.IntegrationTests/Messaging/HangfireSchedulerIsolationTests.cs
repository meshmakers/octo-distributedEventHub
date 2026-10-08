using System.Collections.Concurrent;
using FluentAssertions;
using Hangfire;
using Hangfire.Common;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     AB#5867: two OctoMesh instances on one broker must not store each other's schedules.
/// </summary>
/// <remarks>
///     Before the fix both bot services consumed the un-prefixed queue <c>hangfire</c> as competing
///     consumers; a recurring schedule of one instance was stored by whichever picked it up, in that
///     instance's Hangfire database, where the owning instance's RemoveRecurringJobsByScheduleGroup can
///     never reach it (test-2-dev: doubled cron ticks, one surviving UndeployTriggers). Hangfire itself is
///     replaced by a recorder: what is under test is which instance's scheduler consumer receives the
///     schedule.
/// </remarks>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class HangfireSchedulerIsolationTests(RabbitMqFixture rabbitMq) : IAsyncLifetime
{
    private const string ScheduleGroup = "pipelineTrigger-leasetest";
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly List<ServiceProvider> _providers = [];
    private string _prefixA = null!;
    private string _prefixB = null!;

    public ValueTask InitializeAsync()
    {
        var run = Guid.NewGuid().ToString("N")[..8];
        _prefixA = $"a{run}";
        _prefixB = $"b{run}";
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var provider in _providers)
        {
            await provider.GetRequiredService<IEventHubControl>().StopAsync();
            await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task RecurringSchedules_OfOneInstance_AreStoredOnlyByThatInstancesScheduler()
    {
        // Arrange — two bot services of two instances on the same broker, both hosting the scheduler.
        var (botA, recorderA) = await StartBotServiceAsync(_prefixA);
        var (_, recorderB) = await StartBotServiceAsync(_prefixB);
        var eventHubA = botA.GetRequiredService<IDistributionEventHubService>();

        // Act — instance A schedules ten cron triggers (one per pipeline of its tenant).
        const int count = 10;
        for (var i = 0; i < count; i++)
        {
            await eventHubA.ScheduleRecurringSendAsync(new ScheduledTick { Pipeline = i },
                $"queue:octo::com-controller::lease-trigger",
                new RecurringSchedulingOptions("0 */2 * * * ?", DateTime.Now, null, $"trigger-lease-pipeline{i}",
                    ScheduleGroup, "test", SchedulingMissedEventPolicy.Skip));
        }

        // Assert — every schedule lands in A's scheduler; B never sees one. With the shared queue the
        // split is roughly half/half and A never reaches ten.
        await WaitUntilAsync(() => recorderA.Added.Count == count);
        await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        recorderA.Added.Keys.Should().HaveCount(count)
            .And.OnlyContain(id => id.EndsWith("-" + ScheduleGroup));
        recorderB.Added.Should().BeEmpty();
    }

    /// <summary>
    ///     N7 (test-2-dev, 2026-10-08): an UNCHANGED schedule re-registered over and over — what every
    ///     tenant update and every DeployTriggers does — must be in the own instance's scheduler after
    ///     EVERY cycle.
    /// </summary>
    /// <remarks>
    ///     A re-registration is "remove the group from the own Hangfire database"
    ///     (<c>RemoveRecurringJobsByScheduleGroup</c>, a prefixed command that only the own bot service
    ///     receives) followed by "publish the schedule again". With the shared un-prefixed queue RabbitMQ
    ///     hands consecutive schedules round-robin to the two bot services, so with nothing else on the
    ///     broker every SECOND publish was stored by the other instance while the remove had already
    ///     emptied the own database: measured on test-2-dev as LOST, present, LOST, present … over eight
    ///     DeployTriggers, and the same on tenant updates. Each cycle here waits until the schedule has
    ///     been stored by SOME instance before it looks, so a regression shows the exact pattern instead of
    ///     a timeout.
    /// </remarks>
    [Fact]
    public async Task ReRegisteringAnUnchangedSchedule_KeepsItInTheOwnInstance_InEveryCycle()
    {
        // Arrange
        var (botA, recorderA) = await StartBotServiceAsync(_prefixA);
        var (_, recorderB) = await StartBotServiceAsync(_prefixB);
        var eventHubA = botA.GetRequiredService<IDistributionEventHubService>();
        const string scheduleId = "49240000000000000000d107-lease-49240000000000000000d105";
        var jobId = $"{scheduleId}-{ScheduleGroup}";
        const int cycles = 8;
        var observed = new List<string>();

        // Act
        for (var cycle = 1; cycle <= cycles; cycle++)
        {
            // What the bot's RecurringJobConsumer does on RemoveRecurringJobsByScheduleGroup: drop every
            // job of the group from the OWN database, then answer — only then does the controller publish.
            foreach (var id in recorderA.Added.Keys.Where(k => k.EndsWith(ScheduleGroup)).ToList())
            {
                recorderA.RemoveIfExists(id);
            }

            await eventHubA.ScheduleRecurringSendAsync(new ScheduledTick { Pipeline = 105 },
                "queue:octo::com-controller::lease-trigger",
                new RecurringSchedulingOptions("0 */2 * * * ?", DateTime.Now, null, scheduleId, ScheduleGroup,
                    "G1 lease probe", SchedulingMissedEventPolicy.Skip));

            var expectedDeliveries = cycle;
            await WaitUntilAsync(() => recorderA.AddCalls + recorderB.AddCalls == expectedDeliveries);
            observed.Add(recorderA.Added.ContainsKey(jobId) ? "present" : "LOST");
        }

        // Assert
        observed.Should().HaveCount(cycles).And.OnlyContain(state => state == "present",
            "an unchanged schedule must survive every re-registration (observed: {0})",
            string.Join(", ", observed));
        recorderA.AddCalls.Should().Be(cycles);
        recorderB.AddCalls.Should().Be(0);
    }

    [Fact]
    public async Task Start_RemovesOnlyThisInstancesLegacyBindings_SoANotYetUpdatedInstanceIsUntouched()
    {
        // Arrange — the topology the previous version left on the broker: the shared "hangfire" exchange
        // and queue, with the scheduling exchanges of BOTH instances bound into it.
        await using var connection = await CreateConnectionAsync();
        await using (var setup = await connection.CreateChannelAsync(cancellationToken: Ct))
        {
            await setup.ExchangeDeclareAsync("hangfire", ExchangeType.Fanout, durable: true, cancellationToken: Ct);
            await setup.QueueDeclareAsync("hangfire", durable: true, exclusive: false, autoDelete: false, cancellationToken: Ct);
            await setup.QueueBindAsync("hangfire", "hangfire", string.Empty, cancellationToken: Ct);
            await setup.QueuePurgeAsync("hangfire", Ct);
            foreach (var prefix in new[] { _prefixA, _prefixB })
            {
                var source = $"{prefix}-MassTransit.Scheduling:ScheduleRecurringMessage";
                await setup.ExchangeDeclareAsync(source, ExchangeType.Fanout, durable: true, cancellationToken: Ct);
                await setup.ExchangeBindAsync("hangfire", source, string.Empty, cancellationToken: Ct);
            }
        }

        // Act — instance A is updated and starts; instance B is still on the old version (not started).
        var (botA, recorderA) = await StartBotServiceAsync(_prefixA);
        await botA.GetRequiredService<IDistributionEventHubService>().ScheduleRecurringSendAsync(
            new ScheduledTick { Pipeline = 1 }, "queue:octo::com-controller::lease-trigger",
            new RecurringSchedulingOptions("0 */2 * * * ?", DateTime.Now, null, "trigger-lease-pipeline1",
                ScheduleGroup, "test", SchedulingMissedEventPolicy.Skip));
        await WaitUntilAsync(() => recorderA.Added.Count == 1);

        // B's (old) instance still publishes through its legacy binding.
        await using (var publisher = await connection.CreateChannelAsync(cancellationToken: Ct))
        {
            await publisher.BasicPublishAsync($"{_prefixB}-MassTransit.Scheduling:ScheduleRecurringMessage",
                string.Empty, "from-b"u8.ToArray(), Ct);
        }

        // Assert — the legacy queue only holds B's message: A's schedule no longer leaks into it.
        await using var check = await connection.CreateChannelAsync(cancellationToken: Ct);
        uint legacyDepth = 0;
        await WaitUntilAsync(async () =>
        {
            legacyDepth = (await check.QueueDeclarePassiveAsync("hangfire", Ct)).MessageCount;
            return legacyDepth >= 1;
        });
        await Task.Delay(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);
        legacyDepth = (await check.QueueDeclarePassiveAsync("hangfire", Ct)).MessageCount;

        legacyDepth.Should().Be(1, "only instance B's legacy binding may remain");
        var leaked = await check.BasicGetAsync("hangfire", autoAck: true, Ct);
        leaked.Should().NotBeNull();
        System.Text.Encoding.UTF8.GetString(leaked!.Body.Span).Should().Be("from-b");
    }

    private async Task<(ServiceProvider Provider, RecordingRecurringJobManager Recorder)> StartBotServiceAsync(
        string instancePrefix)
    {
        var recorder = new RecordingRecurringJobManager();
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = instancePrefix;
            o.BrokerHost = rabbitMq.Host;
            o.BrokerPort = (ushort)rabbitMq.Port;
            o.BrokerUser = rabbitMq.Username;
            o.BrokerPassword = rabbitMq.Password;
        });
        services.AddSingleton<IRecurringJobManager>(recorder);
        services.AddSingleton<ITimeZoneResolver, DefaultTimeZoneResolver>();
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = $"BotService-{instancePrefix}";
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddHangfireMessageScheduler();
        });

        // MassTransit's own observer starts a Hangfire server and needs a JobStorage; the recorder
        // replaces Hangfire, so it is not needed here.
        foreach (var d in services.Where(d => d.ServiceType == typeof(IBusObserver) &&
                                              d.ImplementationType?.Name == "HangfireBusObserver").ToList())
        {
            services.Remove(d);
        }

        var provider = services.BuildServiceProvider();
        _providers.Add(provider);
        await provider.GetRequiredService<IEventHubControl>().StartAsync();
        return (provider, recorder);
    }

    private Task<IConnection> CreateConnectionAsync() =>
        new ConnectionFactory
        {
            HostName = rabbitMq.Host, Port = rabbitMq.Port, UserName = rabbitMq.Username,
            Password = rabbitMq.Password
        }.CreateConnectionAsync(Ct);

    private static Task WaitUntilAsync(Func<bool> condition) => WaitUntilAsync(() => Task.FromResult(condition()));

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + DeliveryTimeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Condition not met within " + DeliveryTimeout);
            }

            await Task.Delay(100, Ct);
        }
    }

    public record ScheduledTick
    {
        public int Pipeline { get; init; }
    }

    private sealed class RecordingRecurringJobManager : IRecurringJobManager
    {
        private int _addCalls;

        public ConcurrentDictionary<string, string> Added { get; } = new();

        /// <summary>Every <c>AddOrUpdate</c> this scheduler received, including updates of a present job.</summary>
        public int AddCalls => Volatile.Read(ref _addCalls);

        public void AddOrUpdate(string recurringJobId, Job job, string cronExpression, RecurringJobOptions options)
        {
            Added[recurringJobId] = cronExpression;
            Interlocked.Increment(ref _addCalls);
        }

        public void Trigger(string recurringJobId)
        {
        }

        public void RemoveIfExists(string recurringJobId) => Added.TryRemove(recurringJobId, out _);
    }
}
