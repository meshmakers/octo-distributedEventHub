using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class BroadcastEventTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _serviceProvider = null!;
    private readonly List<ReceivedBroadcastMessage> _receivedMessages = [];

    public BroadcastEventTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async ValueTask InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = $"broadcast-test-{Guid.NewGuid():N}";
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = _rabbitMq.Username;
            o.BrokerPassword = _rabbitMq.Password;
        });

        services.AddSingleton(_receivedMessages);
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "BroadcastTestService";
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddBroadcastEventConsumer<TestBroadcastConsumer, TestBroadcastMessage>();
        });

        _serviceProvider = services.BuildServiceProvider();

        // Start the bus manually
        var eventHubControl = _serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StartAsync();

        // Wait for bus to be ready
        await Task.Delay(1000);
    }

    public async ValueTask DisposeAsync()
    {
        var eventHubControl = _serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StopAsync();
        await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task PublishAsync_BroadcastMessage_IsReceivedByConsumer()
    {
        // Arrange
        var eventHub = _serviceProvider.GetRequiredService<IDistributionEventHubService>();
        var message = new TestBroadcastMessage { Id = Guid.NewGuid().ToString(), Content = "Test broadcast" };

        // Act
        await eventHub.PublishAsync(message);

        // Assert - Wait for message to be consumed
        await TestHelpers.WaitForCondition(
            () => _receivedMessages.Any(m => m.Id == message.Id),
            TimeSpan.FromSeconds(10));

        _receivedMessages.Should().ContainSingle(m => m.Id == message.Id);
    }

    [Fact]
    public async Task PublishAsync_MultipleMessages_AllAreReceived()
    {
        // Arrange
        var eventHub = _serviceProvider.GetRequiredService<IDistributionEventHubService>();
        var messages = Enumerable.Range(1, 3)
            .Select(i => new TestBroadcastMessage { Id = $"multi-{Guid.NewGuid():N}-{i}", Content = $"Message {i}" })
            .ToList();

        // Act
        foreach (var message in messages)
        {
            await eventHub.PublishAsync(message);
            await Task.Delay(100, TestContext.Current.CancellationToken); // Small delay between publishes for stability
        }

        // Assert
        await TestHelpers.WaitForCondition(
            () => messages.All(m => _receivedMessages.Any(r => r.Id == m.Id)),
            TimeSpan.FromSeconds(30));

        foreach (var message in messages)
        {
            _receivedMessages.Should().Contain(m => m.Id == message.Id);
        }
    }
}

public record TestBroadcastMessage
{
    public string Id { get; init; } = null!;
    public string Content { get; init; } = null!;
}

public record ReceivedBroadcastMessage(string Id, DateTime ReceivedAt);

public class TestBroadcastConsumer : IDistributedConsumer<TestBroadcastMessage>
{
    private readonly List<ReceivedBroadcastMessage> _receivedMessages;

    public TestBroadcastConsumer(List<ReceivedBroadcastMessage> receivedMessages)
    {
        _receivedMessages = receivedMessages;
    }

    public Task ConsumeAsync(IDistributedContext<TestBroadcastMessage> context)
    {
        // Consumers run concurrently (default prefetch, two buses): List<T>.Add is not thread-safe and can drop an entry.
        lock (_receivedMessages)
        {
            _receivedMessages.Add(new ReceivedBroadcastMessage(context.Message.Id, DateTime.UtcNow));
        }
        return Task.CompletedTask;
    }
}
