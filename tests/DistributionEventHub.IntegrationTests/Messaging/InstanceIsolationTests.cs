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
public class InstanceIsolationTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _instance1Provider = null!;
    private ServiceProvider _instance2Provider = null!;
    private readonly List<(string Instance, string MessageId)> _receivedMessages = [];
    private string _instance1Prefix = null!;
    private string _instance2Prefix = null!;

    public InstanceIsolationTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async ValueTask InitializeAsync()
    {
        _instance1Prefix = $"instance-a-{Guid.NewGuid():N}";
        _instance2Prefix = $"instance-b-{Guid.NewGuid():N}";

        // Instance 1 with prefix "instance-a"
        _instance1Provider = CreateServiceProvider(_instance1Prefix, "Instance1");

        // Instance 2 with prefix "instance-b"
        _instance2Provider = CreateServiceProvider(_instance2Prefix, "Instance2");

        // Start both buses manually
        var eventHubControl1 = _instance1Provider.GetRequiredService<IEventHubControl>();
        await eventHubControl1.StartAsync();

        var eventHubControl2 = _instance2Provider.GetRequiredService<IEventHubControl>();
        await eventHubControl2.StartAsync();

        // Wait for buses to be ready
        await Task.Delay(1000);
    }

    private ServiceProvider CreateServiceProvider(string instancePrefix, string serviceName)
    {
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

        services.AddSingleton(_receivedMessages);
        services.AddSingleton(instancePrefix); // For consumer identification
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = serviceName;
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddBroadcastEventConsumer<IsolationTestConsumer, IsolationTestMessage>();
        });

        return services.BuildServiceProvider();
    }

    public async ValueTask DisposeAsync()
    {
        var eventHubControl1 = _instance1Provider.GetRequiredService<IEventHubControl>();
        await eventHubControl1.StopAsync();

        var eventHubControl2 = _instance2Provider.GetRequiredService<IEventHubControl>();
        await eventHubControl2.StopAsync();

        await _instance1Provider.DisposeAsync();
        await _instance2Provider.DisposeAsync();
    }

    [Fact]
    public async Task Publish_DifferentInstances_MessagesAreIsolated()
    {
        // Arrange
        var eventHub1 = _instance1Provider.GetRequiredService<IDistributionEventHubService>();
        var eventHub2 = _instance2Provider.GetRequiredService<IDistributionEventHubService>();

        var message1 = new IsolationTestMessage { Id = $"from-instance-1-{Guid.NewGuid():N}" };
        var message2 = new IsolationTestMessage { Id = $"from-instance-2-{Guid.NewGuid():N}" };

        // Act
        await eventHub1.PublishAsync(message1);
        await eventHub2.PublishAsync(message2);

        // Wait for messages
        await Task.Delay(5000, TestContext.Current.CancellationToken);

        // Assert - Each instance should only receive its own message
        var instance1Messages = _receivedMessages.Where(m => m.Instance == _instance1Prefix).ToList();
        var instance2Messages = _receivedMessages.Where(m => m.Instance == _instance2Prefix).ToList();

        instance1Messages.Should().ContainSingle(m => m.MessageId == message1.Id);
        instance1Messages.Should().NotContain(m => m.MessageId == message2.Id);

        instance2Messages.Should().ContainSingle(m => m.MessageId == message2.Id);
        instance2Messages.Should().NotContain(m => m.MessageId == message1.Id);
    }
}

public record IsolationTestMessage
{
    public string Id { get; init; } = null!;
}

public class IsolationTestConsumer : IDistributedConsumer<IsolationTestMessage>
{
    private readonly List<(string Instance, string MessageId)> _receivedMessages;
    private readonly string _instancePrefix;

    public IsolationTestConsumer(List<(string Instance, string MessageId)> receivedMessages, string instancePrefix)
    {
        _receivedMessages = receivedMessages;
        _instancePrefix = instancePrefix;
    }

    public Task ConsumeAsync(IDistributedContext<IsolationTestMessage> context)
    {
        _receivedMessages.Add((_instancePrefix, context.Message.Id));
        return Task.CompletedTask;
    }
}
