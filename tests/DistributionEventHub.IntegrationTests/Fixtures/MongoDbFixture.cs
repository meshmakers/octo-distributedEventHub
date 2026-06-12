using Testcontainers.MongoDb;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;

/// <summary>
///     MongoDB Testcontainer fixture. Single-node, no replica set — this repo's tests
///     don't need transactions.
///
///     Retry pattern matches octo-ai-services / octo-construction-kit-engine-mongodb:
///     container startup occasionally fails transiently on self-hosted DinD CI agents
///     (port races, image-pull stalls, Docker socket hiccups). A fresh container per
///     attempt with a per-attempt hard timeout prevents indefinite hangs.
/// </summary>
public class MongoDbFixture : IAsyncLifetime
{
    private MongoDbContainer? _container;

    public string ConnectionString => Container.GetConnectionString();
    public string Host => Container.Hostname;
    public int Port => Container.GetMappedPublicPort(27017);
    public string Username => "testuser";
    public string Password => "testpassword";

    private MongoDbContainer Container =>
        _container ?? throw new InvalidOperationException("MongoDB container not initialized.");

    public async ValueTask InitializeAsync()
    {
        const int maxAttempts = 3;
        var perAttemptTimeout = TimeSpan.FromMinutes(2);

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            Console.WriteLine($"[MongoDbFixture] StartAsync attempt {attempt}/{maxAttempts}");

            _container = new MongoDbBuilder("mongo:7")
                .WithUsername(Username)
                .WithPassword(Password)
                .Build();

            using var startCts = new CancellationTokenSource(perAttemptTimeout);
            try
            {
                await _container.StartAsync(startCts.Token);
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"[MongoDbFixture] StartAsync attempt {attempt}/{maxAttempts} failed: {ex.GetType().Name}: {ex.Message}");

                try
                {
                    await _container.DisposeAsync();
                }
                catch (Exception disposeEx)
                {
                    Console.WriteLine($"[MongoDbFixture]   Disposal of failed container also threw: {disposeEx.Message}");
                }

                _container = null;

                if (attempt == maxAttempts)
                {
                    throw;
                }

                await Task.Delay(TimeSpan.FromSeconds(2 * attempt));
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
            _container = null;
        }
    }
}

[CollectionDefinition("MongoDB")]
public class MongoDbCollection : ICollectionFixture<MongoDbFixture>
{
}
