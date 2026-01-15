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
public class CommandClientIntegrationTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _consumerServiceProvider = null!;
    private ServiceProvider _clientServiceProvider = null!;
    private string _instancePrefix = null!;

    public CommandClientIntegrationTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async Task InitializeAsync()
    {
        _instancePrefix = $"cmd-test-{Guid.NewGuid():N}";

        // Setup Consumer Service
        var consumerServices = new ServiceCollection();
        consumerServices.AddLogging();
        consumerServices.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = _instancePrefix;
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = _rabbitMq.Username;
            o.BrokerPassword = _rabbitMq.Password;
        });

        consumerServices.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "CommandConsumerService";
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddCommandConsumer<EchoCommandConsumer, EchoRequest>("echo-command");
        });

        _consumerServiceProvider = consumerServices.BuildServiceProvider();

        // Setup Client Service
        var clientServices = new ServiceCollection();
        clientServices.AddLogging();
        clientServices.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = _instancePrefix;
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = _rabbitMq.Username;
            o.BrokerPassword = _rabbitMq.Password;
        });

        clientServices.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "CommandClientService";
            config.AutomaticallyStartBusDuringStartup = false;
            config.AddCommandClient<EchoRequest>("echo-command", TimeSpan.FromSeconds(30));
        });

        _clientServiceProvider = clientServices.BuildServiceProvider();

        // Start the buses manually
        var consumerEventHubControl = _consumerServiceProvider.GetRequiredService<IEventHubControl>();
        await consumerEventHubControl.StartAsync();

        var clientEventHubControl = _clientServiceProvider.GetRequiredService<IEventHubControl>();
        await clientEventHubControl.StartAsync();

        // Wait for services to be ready
        await Task.Delay(1000);
    }

    public async Task DisposeAsync()
    {
        var clientEventHubControl = _clientServiceProvider.GetRequiredService<IEventHubControl>();
        await clientEventHubControl.StopAsync();

        var consumerEventHubControl = _consumerServiceProvider.GetRequiredService<IEventHubControl>();
        await consumerEventHubControl.StopAsync();

        await _clientServiceProvider.DisposeAsync();
        await _consumerServiceProvider.DisposeAsync();
    }

    [Fact]
    public async Task GetResponse_ValidRequest_ReturnsResponse()
    {
        // Arrange
        var client = _clientServiceProvider.GetRequiredService<ICommandClient<EchoRequest>>();
        var request = new EchoRequest { Message = "Hello, World!" };

        // Act
        var response = await client.GetResponse<EchoResponse>(request);

        // Assert
        response.Echo.Should().Be("Echo: Hello, World!");
    }

    [Fact]
    public async Task GetResponse_MultipleRequests_AllSucceed()
    {
        // Arrange
        var client = _clientServiceProvider.GetRequiredService<ICommandClient<EchoRequest>>();
        var tasks = Enumerable.Range(1, 5)
            .Select(i => client.GetResponse<EchoResponse>(new EchoRequest { Message = $"Message {i}" }))
            .ToList();

        // Act
        var responses = await Task.WhenAll(tasks);

        // Assert
        responses.Should().HaveCount(5);
        responses.Select(r => r.Echo).Should().OnlyContain(e => e.StartsWith("Echo:"));
    }

    [Fact]
    public async Task GetResponse_WithDifferentMessages_ReturnsDifferentResponses()
    {
        // Arrange
        var client = _clientServiceProvider.GetRequiredService<ICommandClient<EchoRequest>>();

        // Act
        var response1 = await client.GetResponse<EchoResponse>(new EchoRequest { Message = "First" });
        var response2 = await client.GetResponse<EchoResponse>(new EchoRequest { Message = "Second" });

        // Assert
        response1.Echo.Should().Be("Echo: First");
        response2.Echo.Should().Be("Echo: Second");
    }
}

public record EchoRequest
{
    public string Message { get; init; } = null!;
}

public record EchoResponse
{
    public string Echo { get; init; } = null!;
}

public class EchoCommandConsumer : IDistributedConsumer<EchoRequest>
{
    public async Task ConsumeAsync(IDistributedContext<EchoRequest> context)
    {
        await context.RespondAsync(new EchoResponse { Echo = $"Echo: {context.Message.Message}" });
    }
}
