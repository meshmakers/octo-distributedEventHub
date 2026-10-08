using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
/// Tests for AddRoutedEventConsumer with explicit queue address.
/// These tests verify that messages sent to an explicit queue name are correctly received.
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
[Trait("Category", "RoutedEventExplicitQueue")]
public class RoutedEventWithExplicitQueueTests
{
    private readonly RabbitMqFixture _rabbitMq;

    public RoutedEventWithExplicitQueueTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    /// <summary>
    /// Verifies that when using AddRoutedEventConsumer with an explicit queue name,
    /// messages sent via IDistributionEventHubService.SendAsync are correctly received.
    /// </summary>
    [Fact]
    public async Task SendAsync_WithExplicitQueueName_MessageIsReceived()
    {
        // Arrange
        var instancePrefix = $"explicit-queue-test1-{Guid.NewGuid():N}";
        var receivedMessages = new List<string>();

        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = instancePrefix;
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = _rabbitMq.Username;
            o.BrokerPassword = _rabbitMq.Password;
        });

        services.AddSingleton(receivedMessages);
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "ExplicitQueueTestService1";
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddRoutedEventConsumer<ExplicitQueueConsumer1, ExplicitQueueMessage1>("my-explicit-queue-1");
        });

        var serviceProvider = services.BuildServiceProvider();
        var eventHubControl = serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StartAsync(TestContext.Current.CancellationToken);
        await Task.Delay(2000, TestContext.Current.CancellationToken);

        try
        {
            var eventHub = serviceProvider.GetRequiredService<IDistributionEventHubService>();
            var message = new ExplicitQueueMessage1 { Id = Guid.NewGuid().ToString() };
            var destinationUri = new Uri("queue:my-explicit-queue-1");

            // Act
            await eventHub.SendAsync(destinationUri, message);

            // Assert - Message should be received
            await TestHelpers.WaitForCondition(
                () => receivedMessages.Contains(message.Id),
                TimeSpan.FromSeconds(10));

            receivedMessages.Should().Contain(message.Id);
        }
        finally
        {
            await eventHubControl.StopAsync(TestContext.Current.CancellationToken);
            await serviceProvider.DisposeAsync();
        }
    }

    /// <summary>
    /// Verifies that EndpointConvention mapping works correctly with explicit queue names.
    /// Using bus.Send() should route to the explicit queue.
    /// </summary>
    [Fact]
    public async Task BusSend_WithEndpointConvention_MessageIsReceived()
    {
        // Arrange
        var instancePrefix = $"explicit-queue-test2-{Guid.NewGuid():N}";
        var receivedMessages = new List<string>();

        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = instancePrefix;
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = _rabbitMq.Username;
            o.BrokerPassword = _rabbitMq.Password;
        });

        services.AddSingleton(receivedMessages);
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "ExplicitQueueTestService2";
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddRoutedEventConsumer<ExplicitQueueConsumer2, ExplicitQueueMessage2>("my-explicit-queue-2");
        });

        var serviceProvider = services.BuildServiceProvider();
        var eventHubControl = serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StartAsync(TestContext.Current.CancellationToken);
        await Task.Delay(2000, TestContext.Current.CancellationToken);

        try
        {
            var bus = serviceProvider.GetRequiredService<IBus>();
            var message = new ExplicitQueueMessage2 { Id = Guid.NewGuid().ToString() };

            // Act - Use Send which relies on EndpointConvention mapping
            await bus.Send(message, TestContext.Current.CancellationToken);

            // Assert - Message should be received via EndpointConvention
            await TestHelpers.WaitForCondition(
                () => receivedMessages.Contains(message.Id),
                TimeSpan.FromSeconds(10));

            receivedMessages.Should().Contain(message.Id);
        }
        finally
        {
            await eventHubControl.StopAsync(TestContext.Current.CancellationToken);
            await serviceProvider.DisposeAsync();
        }
    }

    /// <summary>
    /// Verifies that multiple messages sent to an explicit queue are all received.
    /// </summary>
    [Fact]
    public async Task SendAsync_MultipleMessages_AllAreReceived()
    {
        // Arrange
        var instancePrefix = $"explicit-queue-test3-{Guid.NewGuid():N}";
        var receivedMessages = new List<string>();

        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = instancePrefix;
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = _rabbitMq.Username;
            o.BrokerPassword = _rabbitMq.Password;
        });

        services.AddSingleton(receivedMessages);
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "ExplicitQueueTestService3";
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddRoutedEventConsumer<ExplicitQueueConsumer3, ExplicitQueueMessage3>("my-explicit-queue-3");
        });

        var serviceProvider = services.BuildServiceProvider();
        var eventHubControl = serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StartAsync(TestContext.Current.CancellationToken);
        await Task.Delay(2000, TestContext.Current.CancellationToken);

        try
        {
            var eventHub = serviceProvider.GetRequiredService<IDistributionEventHubService>();
            var messages = Enumerable.Range(1, 3)
                .Select(i => new ExplicitQueueMessage3 { Id = $"msg-{Guid.NewGuid():N}-{i}" })
                .ToList();
            var destinationUri = new Uri("queue:my-explicit-queue-3");

            // Act
            foreach (var message in messages)
            {
                await eventHub.SendAsync(destinationUri, message);
            }

            // Assert - All messages should be received
            await TestHelpers.WaitForCondition(
                () => messages.All(m => receivedMessages.Contains(m.Id)),
                TimeSpan.FromSeconds(15));

            foreach (var message in messages)
            {
                receivedMessages.Should().Contain(message.Id);
            }
        }
        finally
        {
            await eventHubControl.StopAsync(TestContext.Current.CancellationToken);
            await serviceProvider.DisposeAsync();
        }
    }
}

// Separate message types for each test to avoid EndpointConvention conflicts
public record ExplicitQueueMessage1 { public string Id { get; init; } = null!; }
public record ExplicitQueueMessage2 { public string Id { get; init; } = null!; }
public record ExplicitQueueMessage3 { public string Id { get; init; } = null!; }

public class ExplicitQueueConsumer1 : IDistributedConsumer<ExplicitQueueMessage1>
{
    private readonly List<string> _receivedMessages;
    public ExplicitQueueConsumer1(List<string> receivedMessages) => _receivedMessages = receivedMessages;
    public Task ConsumeAsync(IDistributedContext<ExplicitQueueMessage1> context)
    {
        // Consumers run concurrently (default prefetch, two buses): List<T>.Add is not thread-safe and can drop an entry.
        lock (_receivedMessages)
        {
            _receivedMessages.Add(context.Message.Id);
        }
        return Task.CompletedTask;
    }
}

public class ExplicitQueueConsumer2 : IDistributedConsumer<ExplicitQueueMessage2>
{
    private readonly List<string> _receivedMessages;
    public ExplicitQueueConsumer2(List<string> receivedMessages) => _receivedMessages = receivedMessages;
    public Task ConsumeAsync(IDistributedContext<ExplicitQueueMessage2> context)
    {
        // Consumers run concurrently (default prefetch, two buses): List<T>.Add is not thread-safe and can drop an entry.
        lock (_receivedMessages)
        {
            _receivedMessages.Add(context.Message.Id);
        }
        return Task.CompletedTask;
    }
}

public class ExplicitQueueConsumer3 : IDistributedConsumer<ExplicitQueueMessage3>
{
    private readonly List<string> _receivedMessages;
    public ExplicitQueueConsumer3(List<string> receivedMessages) => _receivedMessages = receivedMessages;
    public Task ConsumeAsync(IDistributedContext<ExplicitQueueMessage3> context)
    {
        // Consumers run concurrently (default prefetch, two buses): List<T>.Add is not thread-safe and can drop an entry.
        lock (_receivedMessages)
        {
            _receivedMessages.Add(context.Message.Id);
        }
        return Task.CompletedTask;
    }
}
