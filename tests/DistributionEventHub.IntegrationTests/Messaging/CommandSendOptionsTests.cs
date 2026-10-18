using System.Collections.Concurrent;
using System.Diagnostics;
using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     Integration tests for the CK-method messaging primitives (contract AB#5671 / M1):
///     <see cref="CommandSendOptions" /> (mandatory, TTL, timeout) and
///     <see cref="CommandConsumerOptions" /> on dynamically registered command consumers.
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class CommandSendOptionsTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private readonly List<ServiceProvider> _providers = [];
    private string _instancePrefix = null!;

    public CommandSendOptionsTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public ValueTask InitializeAsync()
    {
        _instancePrefix = $"cmdopt-{Guid.NewGuid():N}";
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var provider in _providers)
        {
            try
            {
                await provider.GetRequiredService<IEventHubControl>().StopAsync();
            }
            catch
            {
                // ignore
            }

            await provider.DisposeAsync();
        }
    }

    private async Task<ServiceProvider> StartBusAsync(string serviceName)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = _instancePrefix;
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = _rabbitMq.Username;
            o.BrokerPassword = _rabbitMq.Password;
        });
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = serviceName;
            config.AutomaticallyStartBusDuringStartup = false;
        });

        var provider = services.BuildServiceProvider();
        _providers.Add(provider);
        await provider.GetRequiredService<IEventHubControl>().StartAsync(TestContext.Current.CancellationToken);
        return provider;
    }

    private static async Task<EndpointHandle> RegisterAsync<TMessage>(ServiceProvider host, string commandName,
        ExecuteCommandHandler<TMessage> handler, CommandConsumerOptions options) where TMessage : class
    {
        var handle = host.GetRequiredService<IEventHubControl>().RegisterCommandConsumer(commandName, handler, options);
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
        return handle;
    }

    private static void Log(string message)
    {
        TestContext.Current.TestOutputHelper?.WriteLine(message);
        var file = Environment.GetEnvironmentVariable("OCTO_DEH_TEST_LOG");
        if (!string.IsNullOrEmpty(file))
        {
            File.AppendAllText(file, $"{DateTime.Now:HH:mm:ss.fff} {message}{Environment.NewLine}");
        }
    }

    private static string NewCommandName() => $"octo::method::test::type::m-{Guid.NewGuid():N}";

    private static ExecuteCommandHandler<MethodTestRequest> EchoHandler(string instance,
        ConcurrentBag<string>? received = null)
    {
        return async (message, respond) =>
        {
            received?.Add(instance);
            await respond(new MethodTestResponse { Value = message.Value, Instance = instance });
        };
    }

    [Fact]
    public async Task Mandatory_NoQueueBound_ThrowsCommandNotRoutableFast()
    {
        var client = await StartBusAsync("MandatoryClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();
        var commandName = NewCommandName();

        // warm-up: first send on a fresh bus pays for channel/exchange setup
        await FluentActions.Invoking(() => hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(
                commandName, new MethodTestRequest { Value = "warmup" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken))
            .Should().ThrowAsync<CommandNotRoutableException>();

        var sw = Stopwatch.StartNew();
        var act = () => hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(
            commandName, new MethodTestRequest { Value = "x" },
            new CommandSendOptions
                { Mandatory = true, Timeout = TimeSpan.FromSeconds(10), TimeToLive = TimeSpan.FromSeconds(10) },
            TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<CommandNotRoutableException>();
        sw.Stop();
        Log($"Mandatory return after {sw.ElapsedMilliseconds} ms");
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task SendRightAfterRegistration_WithoutWaitingForReady_IsDeliveredOrNotRoutable_NeverTimesOut()
    {
        var host = await StartBusAsync("RaceHost");
        var client = await StartBusAsync("RaceClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();

        for (var i = 0; i < 5; i++)
        {
            var commandName = NewCommandName();
            await using var handle = host.GetRequiredService<IEventHubControl>().RegisterCommandConsumer(commandName,
                EchoHandler("race"), new CommandConsumerOptions());

            // deliberately no WaitUntilReadyAsync: the queue and its binding may not exist yet
            var started = Stopwatch.StartNew();
            try
            {
                var response = await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                    new MethodTestRequest { Value = "race" },
                    new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                    TestContext.Current.CancellationToken);
                response.Value.Should().Be("race");
            }
            catch (CommandNotRoutableException)
            {
                // typed, immediate failure is the contract when the binding is not there yet
            }

            started.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(8), "a race must never end in the timeout");

            // once ready, the very same endpoint accepts mandatory sends without exception
            await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
            var ready = await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                new MethodTestRequest { Value = "ready" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken);
            ready.Value.Should().Be("ready");
        }
    }

    [Fact]
    public async Task Mandatory_AfterEndpointDisposed_ThrowsCommandNotRoutable()
    {
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("MandatoryClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();
        var commandName = NewCommandName();

        var handle = await RegisterAsync(handlerHost, commandName, EchoHandler("h1"), new CommandConsumerOptions { PrefetchCount = 16 });

        var options = new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) };
        var ok = await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
            new MethodTestRequest { Value = "before" }, options, TestContext.Current.CancellationToken);
        ok.Value.Should().Be("before");

        await handle.DisposeAsync();

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            var sw = Stopwatch.StartNew();
            Exception? caught = null;
            try
            {
                await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(
                    commandName, new MethodTestRequest { Value = "after" }, options,
                    TestContext.Current.CancellationToken);
            }
            catch (Exception e)
            {
                caught = e;
            }

            sw.Stop();
            Log($"[dispose] attempt {attempt}: {caught?.GetType().Name} after {sw.ElapsedMilliseconds} ms");
            caught.Should().BeOfType<CommandNotRoutableException>();
            sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(500));
        }
    }

    [Fact]
    public async Task NotMandatory_NoQueueBound_ThrowsDistributionTimeout()
    {
        var client = await StartBusAsync("NonMandatoryClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();

        var sw = Stopwatch.StartNew();
        var act = () => hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(
            NewCommandName(), new MethodTestRequest { Value = "x" },
            new CommandSendOptions { Mandatory = false, Timeout = TimeSpan.FromSeconds(2) },
            TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<DistributionTimeoutException>();
        sw.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(1.9));
    }

    [Fact]
    public async Task Cancellation_IsRethrownUnchanged()
    {
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("CancelClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();
        var commandName = NewCommandName();

        await using var handle = await RegisterAsync<MethodTestRequest>(handlerHost, commandName, async (_, _) =>
            {
                // never responds
                await Task.Delay(TimeSpan.FromSeconds(5));
            }, new CommandConsumerOptions());

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var act = () => hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
            new MethodTestRequest { Value = "x" },
            new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) }, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ThrowingDynamicConsumer_IsRetriedByBus_FourExecutions()
    {
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("RetryClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();
        var commandName = NewCommandName();
        var executions = 0;

        await using var handle = await RegisterAsync<MethodTestRequest>(handlerHost, commandName, (_, _) =>
            {
                Interlocked.Increment(ref executions);
                throw new InvalidOperationException("boom");
            }, new CommandConsumerOptions());

        // bus-level UseMessageRetry(1s, 5s, 15s) → 1 + 3 retries ≈ 21 s, then the fault reaches the requester
        var act = () => hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
            new MethodTestRequest { Value = "x" },
            new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(60) },
            TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<DistributedOperationFailedException>();
        executions.Should().Be(4);
    }

    [Fact]
    public async Task NonThrowingWrappedConsumer_IsExecutedExactlyOnce()
    {
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("OnceClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();
        var commandName = NewCommandName();
        var executions = 0;

        await using var handle = await RegisterAsync<MethodTestRequest>(handlerHost, commandName, async (_, respond) =>
            {
                Interlocked.Increment(ref executions);
                try
                {
                    throw new InvalidOperationException("boom");
                }
                catch (Exception)
                {
                    await respond(new MethodTestResponse { Value = "failed", Instance = "h" });
                }
            }, new CommandConsumerOptions());

        var response = await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
            new MethodTestRequest { Value = "x" },
            new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
            TestContext.Current.CancellationToken);

        response.Value.Should().Be("failed");
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        executions.Should().Be(1);
    }

    [Fact]
    public async Task ConfigureConsumeTopologyFalse_PublishDoesNotReachQueue()
    {
        var handlerHost = await StartBusAsync("HandlerHost");
        var publisher = await StartBusAsync("Publisher");
        var withoutTopology = new ConcurrentBag<string>();
        var withTopology = new ConcurrentBag<string>();

        await using var h1 = await RegisterAsync<TopologyTestMessage>(handlerHost, NewCommandName(),
            (_, _) =>
            {
                withoutTopology.Add("x");
                return Task.CompletedTask;
            }, new CommandConsumerOptions { ConfigureConsumeTopology = false });
        await using var h2 = await RegisterAsync<TopologyTestMessage>(handlerHost, NewCommandName(),
            (_, _) =>
            {
                withTopology.Add("x");
                return Task.CompletedTask;
            }, new CommandConsumerOptions { ConfigureConsumeTopology = true });

        await publisher.GetRequiredService<IDistributionEventHubService>()
            .PublishAsync(new TopologyTestMessage { Value = "published" }, TestContext.Current.CancellationToken);

        // the control endpoint proves the publish was delivered at all
        await TestHelpers.WaitForCondition(() => !withTopology.IsEmpty, TimeSpan.FromSeconds(10));
        await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        withoutTopology.Should().BeEmpty();
    }

    [Fact]
    public async Task TwoConsumersSameCommandName_AreCompetingConsumers()
    {
        var hostA = await StartBusAsync("HandlerHostA");
        var hostB = await StartBusAsync("HandlerHostB");
        var client = await StartBusAsync("CompetingClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();
        var commandName = NewCommandName();
        var received = new ConcurrentBag<string>();
        var options = new CommandConsumerOptions { PrefetchCount = 1 };

        Log("register A");
        await using var hA = await RegisterAsync(hostA, commandName, EchoHandler("A", received), options);
        Log("register B");
        await using var hB = await RegisterAsync(hostB, commandName, EchoHandler("B", received), options);
        Log("sending");

        var sendOptions = new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) };
        var responses = await Task.WhenAll(Enumerable.Range(0, 20).Select(i =>
            hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                new MethodTestRequest { Value = i.ToString() }, sendOptions,
                TestContext.Current.CancellationToken)));

        Log("done A=" + received.Count(r => r == "A"));
        responses.Should().HaveCount(20);
        received.Should().HaveCount(20);
        received.Count(r => r == "A").Should().BeGreaterThan(0);
        received.Count(r => r == "B").Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task SendLatency_MandatoryVersusNonMandatory_IsRecorded()
    {
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("LatencyClient");
        var hub = client.GetRequiredService<IDistributionEventHubService>();
        var commandName = NewCommandName();

        await using var handle = await RegisterAsync(handlerHost, commandName, EchoHandler("h"), new CommandConsumerOptions { PrefetchCount = 16 });

        async Task<double[]> MeasureAsync(bool mandatory)
        {
            var options = new CommandSendOptions { Mandatory = mandatory, Timeout = TimeSpan.FromSeconds(10) };
            var samples = new List<double>();
            for (var i = 0; i < 60; i++)
            {
                var sw = Stopwatch.StartNew();
                await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                    new MethodTestRequest { Value = i.ToString() }, options, TestContext.Current.CancellationToken);
                sw.Stop();
                if (i >= 10)
                {
                    samples.Add(sw.Elapsed.TotalMilliseconds); // skip warm-up
                }
            }

            samples.Sort();
            return samples.ToArray();
        }

        var plain = await MeasureAsync(false);
        var mandatory = await MeasureAsync(true);

        string Describe(double[] s) =>
            $"p50={s[s.Length / 2]:F2} ms p95={s[(int)(s.Length * 0.95)]:F2} ms avg={s.Average():F2} ms";

        Log($"Round trip non-mandatory: {Describe(plain)}");
        Log($"Round trip mandatory:     {Describe(mandatory)}");
        Log($"[latency] non-mandatory {Describe(plain)}; mandatory {Describe(mandatory)}");
    }
}

public record MethodTestRequest
{
    public string Value { get; init; } = null!;
}

public record MethodTestResponse
{
    public string Value { get; init; } = null!;
    public string Instance { get; init; } = null!;
}

public record TopologyTestMessage
{
    public string Value { get; init; } = null!;
}
