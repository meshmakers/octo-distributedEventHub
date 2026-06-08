using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
/// Verifies the queue topology declared by <see cref="IEventHubControl.RegisterCommandConsumer{TMessage}"/> —
/// the runtime registration path used by the ETL trigger nodes (FromPipelineDataEvent / FromExecutePipelineCommand /
/// FromSendNotification).
///
/// Regression guard for the production RESOURCE_LOCKED (AMQP 405) storm: command queues must be
/// <b>non-exclusive</b> (so a reconnecting consumer can re-declare without being locked out by the not-yet-reaped
/// previous owner) and carry <c>x-single-active-consumer</c> (to keep the "exactly one adapter handles this command"
/// guarantee without an exclusive queue).
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class CommandConsumerTopologyTests : IAsyncLifetime
{
    private static readonly HttpClient HttpClient = new();

    private readonly RabbitMqFixture _rabbitMq;
    private readonly List<ServiceProvider> _providers = [];
    private string _instancePrefix = null!;

    public CommandConsumerTopologyTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public ValueTask InitializeAsync()
    {
        _instancePrefix = $"sac-test-{Guid.NewGuid():N}";
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
            catch (Exception)
            {
                // Best-effort teardown: a bus that never fully started should not fail the test run.
            }

            await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task RegisterCommandConsumer_DeclaresNonExclusiveSingleActiveConsumerQueue()
    {
        const string command = "topology-cmd";
        var provider = await CreateStartedProviderAsync("TopologyService");
        var hub = provider.GetRequiredService<IEventHubControl>();

        await using var handle = hub.RegisterCommandConsumer<TestCommand>(command,
            (msg, respond) => respond(new TestCommandResponse { Reply = msg.Text }));

        var queue = await WaitForQueueAsync(QueueName(command), _ => true, TimeSpan.FromSeconds(15));

        queue.Exclusive.Should().BeFalse(
            "a command queue must be re-declarable by a reconnecting consumer; an exclusive queue causes RESOURCE_LOCKED");
        queue.Arguments.Should().ContainKey("x-single-active-consumer");
        IsTrue(queue.Arguments["x-single-active-consumer"]).Should().BeTrue(
            "single-active-consumer preserves the single-handler guarantee without an exclusive queue");
    }

    [Fact]
    public async Task RegisterCommandConsumer_SecondConnectionOnSameQueue_AttachesWithoutResourceLock()
    {
        const string command = "overlap-cmd";
        var queueName = QueueName(command);

        // Connection #1 owns the queue.
        var provider1 = await CreateStartedProviderAsync("OwnerService");
        var hub1 = provider1.GetRequiredService<IEventHubControl>();
        await using var handle1 = hub1.RegisterCommandConsumer<TestCommand>(command,
            (msg, respond) => respond(new TestCommandResponse { Reply = $"one:{msg.Text}" }));

        await WaitForQueueAsync(queueName, q => q.Consumers >= 1, TimeSpan.FromSeconds(15));

        // Connection #2 attaches to the SAME queue — this is the reconnect/redeploy overlap that used to throw
        // RESOURCE_LOCKED. With an exclusive queue the second connection could never declare/attach, so the
        // consumer count would stay at 1; non-exclusive + SAC lets both attach (one active, one standby).
        var provider2 = await CreateStartedProviderAsync("StandbyService");
        var hub2 = provider2.GetRequiredService<IEventHubControl>();
        await using var handle2 = hub2.RegisterCommandConsumer<TestCommand>(command,
            (msg, respond) => respond(new TestCommandResponse { Reply = $"two:{msg.Text}" }));

        var queue = await WaitForQueueAsync(queueName, q => q.Consumers >= 2, TimeSpan.FromSeconds(15));

        queue.Exclusive.Should().BeFalse();
        queue.Consumers.Should().BeGreaterThanOrEqualTo(2,
            "both an active and a standby consumer must be able to attach to a single-active-consumer queue");
    }

    [Fact]
    public async Task RegisterCommandConsumer_CommandStillRoundTrips()
    {
        const string command = "roundtrip-cmd";
        var provider = await CreateStartedProviderAsync("RoundTripService");
        var hub = provider.GetRequiredService<IEventHubControl>();

        await using var handle = hub.RegisterCommandConsumer<TestCommand>(command,
            (msg, respond) => respond(new TestCommandResponse { Reply = $"Echo: {msg.Text}" }));

        await WaitForQueueAsync(QueueName(command), q => q.Consumers >= 1, TimeSpan.FromSeconds(15));

        // Single-active-consumer must not change request/response semantics.
        var service = provider.GetRequiredService<IDistributionEventHubService>();
        var response = await service.GetCommandResponseAsync<TestCommand, TestCommandResponse>(
            command, new TestCommand { Text = "ping" },
            TestContext.Current.CancellationToken, TimeSpan.FromSeconds(30));

        response.Reply.Should().Be("Echo: ping");
    }

    [Fact]
    public async Task RegisterRoutedEventConsumer_TopicQueue_IsNonExclusive()
    {
        // The pub/sub leg of FromPipelineDataEvent. Per-subscriber GUID-named queues bound to a topic
        // exchange; they must be non-exclusive so the same endpoint can re-declare them after a transport
        // reconnect without RESOURCE_LOCKED.
        var exchange = $"topic-excl-{Guid.NewGuid():N}";
        var provider = await CreateStartedProviderAsync("TopicExclusiveService");
        var hub = provider.GetRequiredService<IEventHubControl>();

        await using var handle = hub.RegisterRoutedEventConsumer<TestCommand>(exchange, "pipeline.123",
            _ => Task.CompletedTask);

        // Queue name is {prefix}-{exchange}-{routingKey}-{guid}; locate it by its unique exchange segment.
        var queue = await WaitForAnyQueueAsync($"{_instancePrefix.ToLower()}-{exchange}", TimeSpan.FromSeconds(15));

        queue.Exclusive.Should().BeFalse(
            "a per-subscriber topic queue must be re-declarable on reconnect — an exclusive one causes RESOURCE_LOCKED");
    }

    private async Task<ServiceProvider> CreateStartedProviderAsync(string uniqueServiceAddress)
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
            config.UniqueServiceAddress = uniqueServiceAddress;
            config.AutomaticallyStartBusDuringStartup = false;
        });

        var provider = services.BuildServiceProvider();
        _providers.Add(provider);

        await provider.GetRequiredService<IEventHubControl>().StartAsync();
        return provider;
    }

    private string QueueName(string commandName) => $"{_instancePrefix.ToLower()}-{commandName}";

    private async Task<QueueInfo> WaitForQueueAsync(string queueName, Func<QueueInfo, bool> predicate, TimeSpan timeout)
    {
        QueueInfo? last = null;
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            last = await TryGetQueueAsync(queueName);
            if (last != null && predicate(last))
            {
                return last;
            }

            await Task.Delay(200);
        }

        var state = last == null
            ? "<queue absent>"
            : $"exclusive={last.Exclusive}, consumers={last.Consumers}, sac={last.Arguments.ContainsKey("x-single-active-consumer")}";
        throw new TimeoutException(
            $"Queue '{queueName}' did not reach the expected state within {timeout.TotalSeconds}s (last seen: {state}).");
    }

    private async Task<QueueInfo> WaitForAnyQueueAsync(string namePart, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var match = (await ListQueuesAsync()).FirstOrDefault(q => q.Name.Contains(namePart));
            if (match != null)
            {
                return match;
            }

            await Task.Delay(200);
        }

        throw new TimeoutException($"No queue whose name contains '{namePart}' appeared within {timeout.TotalSeconds}s.");
    }

    private async Task<IReadOnlyList<QueueInfo>> ListQueuesAsync()
    {
        var url = $"http://{_rabbitMq.Host}:{_rabbitMq.ManagementPort}/api/queues/%2F";
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes($"{_rabbitMq.Username}:{_rabbitMq.Password}")));

        using var response = await HttpClient.SendAsync(request);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync();
        using var doc = await JsonDocument.ParseAsync(stream);

        var result = new List<QueueInfo>();
        foreach (var element in doc.RootElement.EnumerateArray())
        {
            result.Add(ParseQueue(element));
        }

        return result;
    }

    private async Task<QueueInfo?> TryGetQueueAsync(string queueName)
    {
        // RabbitMQ HTTP management API: vhost "/" must be URL-encoded as %2F.
        var url = $"http://{_rabbitMq.Host}:{_rabbitMq.ManagementPort}/api/queues/%2F/{Uri.EscapeDataString(queueName)}";
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes($"{_rabbitMq.Username}:{_rabbitMq.Password}")));

        using var response = await HttpClient.SendAsync(request);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync();
        using var doc = await JsonDocument.ParseAsync(stream);
        return ParseQueue(doc.RootElement);
    }

    private static QueueInfo ParseQueue(JsonElement root)
    {
        var name = root.TryGetProperty("name", out var n) ? n.GetString() ?? "" : "";
        var exclusive = root.GetProperty("exclusive").GetBoolean();
        var consumers = root.TryGetProperty("consumers", out var c) ? c.GetInt32() : 0;

        var arguments = new Dictionary<string, JsonElement>();
        if (root.TryGetProperty("arguments", out var args) && args.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in args.EnumerateObject())
            {
                arguments[property.Name] = property.Value.Clone();
            }
        }

        return new QueueInfo(name, exclusive, consumers, arguments);
    }

    // The management API may serialize a boolean queue argument as JSON true or as the string "true".
    private static bool IsTrue(JsonElement value) =>
        value.ValueKind == JsonValueKind.True
        || (value.ValueKind == JsonValueKind.String && string.Equals(value.GetString(), "true", StringComparison.OrdinalIgnoreCase));

    private sealed record QueueInfo(string Name, bool Exclusive, int Consumers, Dictionary<string, JsonElement> Arguments);
}

public record TestCommand
{
    public string Text { get; init; } = null!;
}

public record TestCommandResponse
{
    public string Reply { get; init; } = null!;
}
