using Testcontainers.MongoDb;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;

public class MongoDbFixture : IAsyncLifetime
{
    private readonly MongoDbContainer _container;

    public string ConnectionString => _container.GetConnectionString();
    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(27017);
    public string Username => "testuser";
    public string Password => "testpassword";

    public MongoDbFixture()
    {
        _container = new MongoDbBuilder("mongo:7")
            .WithUsername(Username)
            .WithPassword(Password)
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

[CollectionDefinition("MongoDB")]
public class MongoDbCollection : ICollectionFixture<MongoDbFixture>
{
}
