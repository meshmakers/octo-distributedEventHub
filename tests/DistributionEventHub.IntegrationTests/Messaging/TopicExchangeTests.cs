using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class TopicExchangeTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _serviceProvider = null!;
    private readonly List<ReceivedTopicMessage> _receivedMessages = [];
    private string _instancePrefix = null!;

    public TopicExchangeTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async ValueTask InitializeAsync()
    {
        _instancePrefix = $"topic-test-{Guid.NewGuid():N}";

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
            config.UniqueServiceAddress = "TopicTestService";
            config.AutomaticallyStartBusDuringStartup = false;
        });

        _serviceProvider = services.BuildServiceProvider();

        var eventHubControl = _serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StartAsync();
        await Task.Delay(1000);
    }

    public async ValueTask DisposeAsync()
    {
        var eventHubControl = _serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StopAsync();
        await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task SendToExchangeAsync_WithMatchingRoutingKey_IsReceivedByConsumer()
    {
        // Arrange
        var eventHubControl = _serviceProvider.GetRequiredService<IEventHubControl>();
        var eventHubService = _serviceProvider.GetRequiredService<IDistributionEventHubService>();

        var exchangeName = "test-topic-exchange";
        var routingKey = "tenant.events.created";
        var messageId = Guid.NewGuid().ToString();

        // Register consumer with matching routing key pattern
        await using var handle = eventHubControl.RegisterRoutedEventConsumer<TestTopicMessage>(
            exchangeName, "tenant.events.#",
            msg =>
            {
                _receivedMessages.Add(new ReceivedTopicMessage(msg.Id, msg.RoutingKey));
                return Task.CompletedTask;
            });

        await Task.Delay(500, TestContext.Current.CancellationToken); // Wait for consumer to be ready

        // Act
        var message = new TestTopicMessage { Id = messageId, RoutingKey = routingKey };
        await eventHubService.SendToExchangeAsync(exchangeName, routingKey, message,
            TestContext.Current.CancellationToken);

        // Assert
        await TestHelpers.WaitForCondition(
            () => _receivedMessages.Any(m => m.Id == messageId),
            TimeSpan.FromSeconds(10));

        _receivedMessages.Should().ContainSingle(m => m.Id == messageId);
    }

    [Fact]
    public async Task SendToExchangeAsync_WithNonMatchingRoutingKey_IsNotReceived()
    {
        // Arrange
        var eventHubControl = _serviceProvider.GetRequiredService<IEventHubControl>();
        var eventHubService = _serviceProvider.GetRequiredService<IDistributionEventHubService>();

        var exchangeName = "test-topic-filter";
        var messageId = Guid.NewGuid().ToString();

        // Register consumer that only listens to "tenant.A.#"
        await using var handle = eventHubControl.RegisterRoutedEventConsumer<TestTopicMessage>(
            exchangeName, "tenant.A.#",
            msg =>
            {
                _receivedMessages.Add(new ReceivedTopicMessage(msg.Id, msg.RoutingKey));
                return Task.CompletedTask;
            });

        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Act: Send with non-matching routing key "tenant.B.events"
        var message = new TestTopicMessage { Id = messageId, RoutingKey = "tenant.B.events" };
        await eventHubService.SendToExchangeAsync(exchangeName, "tenant.B.events", message,
            TestContext.Current.CancellationToken);

        // Assert: Message should NOT be received
        await Task.Delay(2000, TestContext.Current.CancellationToken);
        _receivedMessages.Should().NotContain(m => m.Id == messageId);
    }

    [Fact]
    public void RegisterRoutedEventConsumer_WithTooLongRoutingKey_ThrowsArgumentException()
    {
        // Arrange
        var eventHubControl = _serviceProvider.GetRequiredService<IEventHubControl>();
        var longRoutingKey = new string('a', 300);

        // Act & Assert
        var act = () => eventHubControl.RegisterRoutedEventConsumer<TestTopicMessage>(
            "exchange", longRoutingKey,
            _ => Task.CompletedTask);

        act.Should().Throw<ArgumentException>()
            .WithParameterName("routingKey")
            .Which.Message.Should().Contain("255");
    }
}

public record TestTopicMessage
{
    public string Id { get; init; } = null!;
    public string RoutingKey { get; init; } = null!;
}

public record ReceivedTopicMessage(string Id, string RoutingKey);
