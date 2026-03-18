using Testcontainers.RabbitMq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;

public class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container;

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(5672);
    public string Username => "guest";
    public string Password => "guest";

    public RabbitMqFixture()
    {
        _container = new RabbitMqBuilder("rabbitmq:3-management")
            .WithUsername("guest")
            .WithPassword("guest")
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
