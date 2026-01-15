using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.MongoDB.Configuration.DependencyInjection;
using Meshmakers.Octo.Common.DistributionEventHub.Sagas;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Caching;

[Collection("MongoDB")]
[Trait("Category", "Integration")]
public class MongoDbCachingTests : IAsyncLifetime
{
    private readonly MongoDbFixture _mongoDb;
    private ServiceProvider _serviceProvider = null!;

    public MongoDbCachingTests(MongoDbFixture mongoDb)
    {
        _mongoDb = mongoDb;
    }

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = $"cache-test-{Guid.NewGuid():N}";
            o.RepositoryHost = $"{_mongoDb.Host}:{_mongoDb.Port}";
            o.SystemDatabaseName = "admin"; // Use admin for auth source
            o.RepositoryUseTls = false;
            o.RepositoryUser = _mongoDb.Username;
            o.RepositoryPassword = _mongoDb.Password;
            o.DatabaseAuthenticationSource = "admin";
        });

        services.AddSingleton<ITenantResolver, TestTenantResolver>();
        services.AddDistributionEventHub(config =>
            {
                config.UniqueServiceAddress = "CacheTestService";
                config.AutomaticallyStartBusDuringStartup = false;
            })
            .AddMongoDbRepository();

        _serviceProvider = services.BuildServiceProvider();

        await Task.Delay(1000); // Wait for connections
    }

    public async Task DisposeAsync()
    {
        await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task CreateAndRetrieveStream_RoundTrip_Success()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();
        var content = "Test content for caching"u8.ToArray();
        var stream = new MemoryStream(content);
        var fileName = $"test-{Guid.NewGuid():N}.txt";

        // Act
        var cacheKey = await cacheService.CreateStreamAsync(
            "tenant-1", stream, "text/plain", fileName, TimeSpan.FromHours(1));

        var retrieved = await cacheService.GetCacheStreamByIdAsync("tenant-1", cacheKey);

        // Assert
        cacheKey.Should().NotBeNullOrEmpty();
        retrieved.Should().NotBeNull();
        retrieved!.ContentType.Should().Be("text/plain");
        retrieved.FileName.Should().Be(fileName);

        using var reader = new StreamReader(retrieved.Stream);
        var retrievedContent = await reader.ReadToEndAsync();
        retrievedContent.Should().Be("Test content for caching");
    }

    [Fact]
    public async Task DeleteCacheStream_FileIsRemoved()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();
        var stream = new MemoryStream("Delete me"u8.ToArray());
        var fileName = $"delete-test-{Guid.NewGuid():N}.txt";
        var cacheKey = await cacheService.CreateStreamAsync(
            "tenant-1", stream, "text/plain", fileName);

        // Act
        await cacheService.DeleteCacheStreamAsync("tenant-1", cacheKey);
        var retrieved = await cacheService.GetCacheStreamByIdAsync("tenant-1", cacheKey);

        // Assert
        retrieved.Should().BeNull();
    }

    [Fact]
    public async Task GetCacheStreamByFileName_ReturnsCorrectFile()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();
        var fileName = $"findable-{Guid.NewGuid():N}.txt";
        var stream = new MemoryStream("Find me by name"u8.ToArray());
        await cacheService.CreateStreamAsync(
            "tenant-1", stream, "text/plain", fileName);

        // Act
        var retrieved = await cacheService.GetCacheStreamByFileNameAsync("tenant-1", fileName);

        // Assert
        retrieved.Should().NotBeNull();
        retrieved!.FileName.Should().Be(fileName);
    }

    [Fact]
    public async Task CreateOrUpdateStream_UpdatesExistingFile()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();
        var fileName = $"updateable-{Guid.NewGuid():N}.txt";
        var originalStream = new MemoryStream("Original content"u8.ToArray());
        var updatedStream = new MemoryStream("Updated content"u8.ToArray());

        // Act
        await cacheService.CreateOrUpdateStreamAsync("tenant-1", originalStream, "text/plain", fileName);
        await cacheService.CreateOrUpdateStreamAsync("tenant-1", updatedStream, "text/plain", fileName);

        var retrieved = await cacheService.GetCacheStreamByFileNameAsync("tenant-1", fileName);

        // Assert
        retrieved.Should().NotBeNull();
        using var reader = new StreamReader(retrieved!.Stream);
        var content = await reader.ReadToEndAsync();
        content.Should().Be("Updated content");
    }

    [Fact]
    public async Task GetCacheStreamByIdAsync_NonExistentKey_ReturnsNull()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();

        // Act - Use a valid ObjectId format (24 hex characters) but one that doesn't exist
        var retrieved = await cacheService.GetCacheStreamByIdAsync("tenant-1", "000000000000000000000000");

        // Assert
        retrieved.Should().BeNull();
    }

    [Fact]
    public async Task GetCacheStreamByFileNameAsync_NonExistentFile_ReturnsNull()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();

        // Act
        var retrieved = await cacheService.GetCacheStreamByFileNameAsync("tenant-1", "non-existent-file.xyz");

        // Assert
        retrieved.Should().BeNull();
    }

    [Fact]
    public async Task CreateStreamAsync_WithDifferentContentTypes_StoresCorrectly()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();
        var pdfContent = new byte[] { 0x25, 0x50, 0x44, 0x46 }; // PDF magic bytes
        var stream = new MemoryStream(pdfContent);
        var fileName = $"document-{Guid.NewGuid():N}.pdf";

        // Act
        var cacheKey = await cacheService.CreateStreamAsync(
            "tenant-1", stream, "application/pdf", fileName);

        var retrieved = await cacheService.GetCacheStreamByIdAsync("tenant-1", cacheKey);

        // Assert
        retrieved.Should().NotBeNull();
        retrieved!.ContentType.Should().Be("application/pdf");
    }

    [Fact]
    public void IsConnected_ReturnsTrue_WhenConnected()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();

        // Assert
        cacheService.IsConnected.Should().BeTrue();
    }
}

public class TestTenantResolver : ITenantResolver
{
    public Task<string> GetRepositoryNameAsync(MassTransit.ConsumeContext consumeContext)
    {
        return Task.FromResult("test-cache-repo");
    }

    public Task<string> GetRepositoryNameAsync(string tenantId)
    {
        return Task.FromResult($"cache-{tenantId}");
    }
}
