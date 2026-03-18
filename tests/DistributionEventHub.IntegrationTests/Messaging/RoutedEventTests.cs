using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class RoutedEventTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _serviceProvider = null!;
    private readonly List<ReceivedRoutedMessage> _receivedMessages = [];
    private string _instancePrefix = null!;

    public RoutedEventTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async ValueTask InitializeAsync()
    {
        _instancePrefix = $"routed-test-{Guid.NewGuid():N}";

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

        services.AddSingleton(_receivedMessages);
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "RoutedTestService";
            config.AutomaticallyStartBusDuringStartup = false;
            // Use parameterless AddRoutedEventConsumer which relies on message type-based endpoint naming
            config.AddRoutedEventConsumer<TestRoutedConsumer, TestRoutedMessage>();
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
    public async Task SendAsync_RoutedMessage_IsReceivedByConsumer()
    {
        // Arrange
        var bus = _serviceProvider.GetRequiredService<IBus>();
        var message = new TestRoutedMessage { Id = Guid.NewGuid().ToString(), Data = "Routed data" };

        // Use MassTransit Send directly - the message type routing is configured by AddRoutedEventConsumer
        // The endpoint name follows the pattern: {prefix}-octo::service::{MessageType.FullName}
        var endpointName = $"{_instancePrefix.ToLower()}-octo::service::{typeof(TestRoutedMessage).FullName}";
        var destinationUri = new Uri($"queue:{endpointName}");

        // Act
        var endpoint = await bus.GetSendEndpoint(destinationUri);
        await endpoint.Send(message, TestContext.Current.CancellationToken);

        // Assert - Wait for message to be consumed
        await TestHelpers.WaitForCondition(
            () => _receivedMessages.Any(m => m.Id == message.Id),
            TimeSpan.FromSeconds(10));

        _receivedMessages.Should().ContainSingle(m => m.Id == message.Id);
    }

    [Fact]
    public async Task SendAsync_MultipleMessages_AllAreReceived()
    {
        // Arrange
        var bus = _serviceProvider.GetRequiredService<IBus>();
        // The endpoint name follows the pattern: {prefix}-octo::service::{MessageType.FullName}
        var endpointName = $"{_instancePrefix.ToLower()}-octo::service::{typeof(TestRoutedMessage).FullName}";
        var destinationUri = new Uri($"queue:{endpointName}");
        var messages = Enumerable.Range(1, 3)
            .Select(i => new TestRoutedMessage { Id = $"routed-multi-{Guid.NewGuid():N}-{i}", Data = $"Data {i}" })
            .ToList();

        // Act
        var endpoint = await bus.GetSendEndpoint(destinationUri);
        foreach (var message in messages)
        {
            await endpoint.Send(message, TestContext.Current.CancellationToken);
        }

        // Assert
        await TestHelpers.WaitForCondition(
            () => messages.All(m => _receivedMessages.Any(r => r.Id == m.Id)),
            TimeSpan.FromSeconds(15));

        foreach (var message in messages)
        {
            _receivedMessages.Should().Contain(m => m.Id == message.Id);
        }
    }
}

public record TestRoutedMessage
{
    public string Id { get; init; } = null!;
    public string Data { get; init; } = null!;
}

public record ReceivedRoutedMessage(string Id, DateTime ReceivedAt);

public class TestRoutedConsumer : IDistributedConsumer<TestRoutedMessage>
{
    private readonly List<ReceivedRoutedMessage> _receivedMessages;

    public TestRoutedConsumer(List<ReceivedRoutedMessage> receivedMessages)
    {
        _receivedMessages = receivedMessages;
    }

    public Task ConsumeAsync(IDistributedContext<TestRoutedMessage> context)
    {
        _receivedMessages.Add(new ReceivedRoutedMessage(context.Message.Id, DateTime.UtcNow));
        return Task.CompletedTask;
    }
}
