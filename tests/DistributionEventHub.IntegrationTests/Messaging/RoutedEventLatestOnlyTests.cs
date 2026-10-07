using System.Collections.Concurrent;
using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     AB#5709: a durable per-pipeline trigger queue collects cron ticks while the consuming adapter is down.
///     With <see cref="RoutedEventConsumerOptions.LatestOnly" /> the backlog collapses into one handled
///     message and the handler never runs concurrently with itself — on a queue that was declared without
///     any queue arguments (as producers and older consumers declare it).
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
[Trait("Category", "RoutedEventLatestOnly")]
public class RoutedEventLatestOnlyTests(RabbitMqFixture rabbitMq)
{
    public record Tick(int Sequence);

    [Fact]
    public async Task BacklogWhileConsumerDown_IsHandledOnceWithTheNewestTick()
    {
        await using var host = await StartHostAsync();
        const string queue = "latest-only-backlog";

        // The consumer existed once with the legacy registration (queue declared durable, no arguments) ...
        var legacy = host.Control.RegisterRoutedEventConsumer<Tick>(queue, _ => Task.CompletedTask);
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        // ... then the adapter goes down while the scheduler keeps ticking.
        await legacy.DisposeAsync();

        for (var i = 1; i <= 20; i++)
        {
            await SendInOrderAsync(host, new Uri($"queue:{queue}"), new Tick(i));
        }

        await Task.Delay(1000, TestContext.Current.CancellationToken);

        // The adapter is back, now with the coalescing registration.
        var handled = new ConcurrentQueue<(Tick Tick, RoutedEventDeliveryContext Delivery)>();
        var handle = host.Control.RegisterRoutedEventConsumer<Tick>(queue,
            (tick, delivery) =>
            {
                handled.Enqueue((tick, delivery));
                return Task.CompletedTask;
            }, RoutedEventConsumerOptions.LatestOnly);

        try
        {
            await TestHelpers.WaitForCondition(() => !handled.IsEmpty, TimeSpan.FromSeconds(15));
            // Give any further (wrong) delivery the chance to show up.
            await Task.Delay(2000, TestContext.Current.CancellationToken);

            handled.Should().ContainSingle();
            handled.Single().Tick.Sequence.Should().Be(20, "the newest pending tick survives (drop-head)");
            handled.Single().Delivery.CoalescedMessageCount.Should().Be(19);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task BacklogSentBeforeAnyConsumerExisted_IsHandledOnce()
    {
        await using var host = await StartHostAsync();
        const string queue = "latest-only-producer-declared";

        // Only the producer has declared the queue so far (MassTransit binds queue: addresses on send).
        for (var i = 1; i <= 5; i++)
        {
            await SendInOrderAsync(host, new Uri($"queue:{queue}"), new Tick(i));
        }

        await Task.Delay(1000, TestContext.Current.CancellationToken);

        var handled = new ConcurrentQueue<Tick>();
        var handle = host.Control.RegisterRoutedEventConsumer<Tick>(queue,
            (tick, _) =>
            {
                handled.Enqueue(tick);
                return Task.CompletedTask;
            }, RoutedEventConsumerOptions.LatestOnly);

        try
        {
            await TestHelpers.WaitForCondition(() => !handled.IsEmpty, TimeSpan.FromSeconds(15));
            await Task.Delay(2000, TestContext.Current.CancellationToken);

            handled.Should().ContainSingle().Which.Sequence.Should().Be(5);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task TicksDuringALongExecution_NeverRunConcurrently_AndCollapseIntoOneFollowUp()
    {
        await using var host = await StartHostAsync();
        const string queue = "latest-only-concurrency";

        var running = 0;
        var maxConcurrent = 0;
        var handled = new ConcurrentQueue<Tick>();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var handle = host.Control.RegisterRoutedEventConsumer<Tick>(queue,
            async (tick, _) =>
            {
                var now = Interlocked.Increment(ref running);
                InterlockedMax(ref maxConcurrent, now);
                try
                {
                    handled.Enqueue(tick);
                    if (tick.Sequence == 1)
                    {
                        firstStarted.TrySetResult();
                        await releaseFirst.Task;
                    }
                    else
                    {
                        await Task.Delay(200);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref running);
                }
            }, RoutedEventConsumerOptions.LatestOnly);

        try
        {
            await Task.Delay(1000, TestContext.Current.CancellationToken);
            await SendInOrderAsync(host, new Uri($"queue:{queue}"), new Tick(1));
            await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

            // Three ticks arrive while the first execution is still running.
            for (var i = 2; i <= 4; i++)
            {
                await SendInOrderAsync(host, new Uri($"queue:{queue}"), new Tick(i));
            }

            await Task.Delay(1000, TestContext.Current.CancellationToken);
            maxConcurrent.Should().Be(1);
            releaseFirst.SetResult();

            await TestHelpers.WaitForCondition(() => handled.Count >= 2, TimeSpan.FromSeconds(15));
            await Task.Delay(2000, TestContext.Current.CancellationToken);

            handled.Select(t => t.Sequence).Should().Equal(1, 4);
            maxConcurrent.Should().Be(1);
        }
        finally
        {
            releaseFirst.TrySetResult();
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task DefaultOptions_HandleEveryMessage()
    {
        await using var host = await StartHostAsync();
        const string queue = "latest-only-default";

        for (var i = 1; i <= 10; i++)
        {
            await SendInOrderAsync(host, new Uri($"queue:{queue}"), new Tick(i));
        }

        await Task.Delay(1000, TestContext.Current.CancellationToken);

        var handled = new ConcurrentQueue<Tick>();
        var handle = host.Control.RegisterRoutedEventConsumer<Tick>(queue,
            (tick, delivery) =>
            {
                delivery.CoalescedMessageCount.Should().Be(0);
                handled.Enqueue(tick);
                return Task.CompletedTask;
            }, RoutedEventConsumerOptions.Default);

        try
        {
            await TestHelpers.WaitForCondition(() => handled.Count == 10, TimeSpan.FromSeconds(15));
            handled.Select(t => t.Sequence).Should().BeEquivalentTo(Enumerable.Range(1, 10));
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    ///     <see cref="IDistributionEventHubService.SendAsync{T}" /> returns the send as an inner task; awaiting
    ///     both keeps the enqueue order deterministic, like ticks that are sent minutes apart.
    /// </summary>
    private static async Task SendInOrderAsync(TestHost host, Uri address, Tick tick)
    {
        await await host.Hub.SendAsync(address, tick);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value &&
               Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    private async Task<TestHost> StartHostAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = $"latest-only-{Guid.NewGuid():N}";
            o.BrokerHost = rabbitMq.Host;
            o.BrokerPort = (ushort)rabbitMq.Port;
            o.BrokerUser = rabbitMq.Username;
            o.BrokerPassword = rabbitMq.Password;
        });
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "LatestOnlyTestService";
            config.AutomaticallyStartBusDuringStartup = false;
        });

        var provider = services.BuildServiceProvider();
        var control = provider.GetRequiredService<IEventHubControl>();
        await control.StartAsync(TestContext.Current.CancellationToken);
        return new TestHost(provider, control, provider.GetRequiredService<IDistributionEventHubService>());
    }

    private sealed record TestHost(ServiceProvider Provider, IEventHubControl Control, IDistributionEventHubService Hub)
        : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Control.StopAsync();
            await Provider.DisposeAsync();
        }
    }
}
