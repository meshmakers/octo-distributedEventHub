using Testcontainers.RabbitMq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;

public class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container;

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(5672);

    /// <summary>
    /// Mapped host port of the RabbitMQ HTTP management API (container port 15672).
    /// Used by topology tests to inspect declared queue properties (exclusive flag, arguments).
    /// </summary>
    public int ManagementPort => _container.GetMappedPublicPort(15672);
    public string Username => "guest";
    public string Password => "guest";

    public RabbitMqFixture()
    {
        _container = new RabbitMqBuilder("rabbitmq:3-management")
            .WithUsername("guest")
            .WithPassword("guest")
            .WithPortBinding(15672, true)
            .Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("RabbitMQ")]
public class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>
{
}
