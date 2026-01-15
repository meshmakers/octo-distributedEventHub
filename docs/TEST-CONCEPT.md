# Test Concept - DistributionEventHub

This document describes the testing strategy for the DistributionEventHub library, covering unit tests and integration tests.

## Table of Contents

1. [Test Strategy Overview](#test-strategy-overview)
2. [Project Structure](#project-structure)
3. [Unit Tests](#unit-tests)
4. [Integration Tests](#integration-tests)
5. [Test Infrastructure](#test-infrastructure)
6. [Test Data and Fixtures](#test-data-and-fixtures)
7. [CI/CD Integration](#cicd-integration)

---

## Test Strategy Overview

### Test Pyramid

```
        /\
       /  \       E2E Tests (Manual/Samples)
      /----\
     /      \     Integration Tests (Testcontainers)
    /--------\
   /          \   Unit Tests (Mocked Dependencies)
  /____________\
```

### Coverage Goals

| Layer | Coverage Target | Focus |
|-------|----------------|-------|
| Unit Tests | 80%+ | Business logic, utilities, exception handling |
| Integration Tests | Critical paths | Messaging patterns, MongoDB operations |
| E2E Tests | Happy paths | Sample applications |

### Testing Frameworks

- **xUnit** - Test framework
- **Moq** - Mocking framework
- **FluentAssertions** - Assertion library
- **Testcontainers** - Docker containers for integration tests
- **MassTransit.Testing** - In-memory test harness for messaging

---

## Project Structure

```
tests/
├── DistributionEventHub.Tests/
│   ├── Unit/
│   │   ├── Services/
│   │   │   ├── CommandClientTests.cs
│   │   │   ├── RoutedCommandClientTests.cs
│   │   │   ├── DistributionEventHubServiceTests.cs
│   │   │   └── DistributedCacheServiceTests.cs
│   │   ├── Consumers/
│   │   │   ├── DistributedConsumerTests.cs
│   │   │   └── DistributedContextTests.cs
│   │   ├── Configuration/
│   │   │   ├── DistributionEventHubConfigurationTests.cs
│   │   │   └── ServiceCollectionExtensionsTests.cs
│   │   ├── Utilities/
│   │   │   └── CacheCommonTests.cs
│   │   └── Exceptions/
│   │       ├── DistributionTimeoutExceptionTests.cs
│   │       └── DistributedOperationFailedExceptionTests.cs
│   └── Integration/
│       ├── Messaging/
│       │   ├── BroadcastEventTests.cs
│       │   ├── RoutedEventTests.cs
│       │   └── CommandClientIntegrationTests.cs
│       ├── Caching/
│       │   └── MongoDbCachingTests.cs
│       └── Fixtures/
│           ├── RabbitMqFixture.cs
│           └── MongoDbFixture.cs
└── DistributionEventHub.Tests.csproj
```

---

## Unit Tests

### 1. CacheCommon Utility Tests

**File:** `Unit/Utilities/CacheCommonTests.cs`

```csharp
public class CacheCommonTests
{
    [Theory]
    [InlineData("prod", "prod-")]
    [InlineData("STAGING", "staging-")]
    [InlineData("Dev", "dev-")]
    public void GetInstancePrefix_ReturnsLowercasedPrefix(string input, string expected)
    {
        // Uses reflection to test internal method or make it public for testing
        var result = CacheCommon.GetInstancePrefix(input);
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("prod", "my-queue", "prod-my-queue")]
    [InlineData("staging", "processor", "staging-processor")]
    public void ApplyInstancePrefix_CombinesPrefixAndName(string prefix, string name, string expected)
    {
        var result = CacheCommon.ApplyInstancePrefix(prefix, name);
        result.Should().Be(expected);
    }

    [Fact]
    public void ApplyInstancePrefixToUri_Queue_PrefixesCorrectly()
    {
        var uri = new Uri("queue:my-queue");
        var result = CacheCommon.ApplyInstancePrefixToUri("prod", uri);
        result.ToString().Should().Be("queue:prod-my-queue");
    }

    [Fact]
    public void ApplyInstancePrefixToUri_Exchange_PrefixesCorrectly()
    {
        var uri = new Uri("exchange:my-exchange?temporary=true");
        var result = CacheCommon.ApplyInstancePrefixToUri("prod", uri);
        result.ToString().Should().Be("exchange:prod-my-exchange?temporary=true");
    }

    [Fact]
    public void ApplyInstancePrefixToUri_EmptyPrefix_ReturnsOriginal()
    {
        var uri = new Uri("queue:my-queue");
        var result = CacheCommon.ApplyInstancePrefixToUri("", uri);
        result.Should().Be(uri);
    }

    [Fact]
    public void ApplyInstancePrefixToUri_UnknownScheme_ReturnsOriginal()
    {
        var uri = new Uri("http://example.com");
        var result = CacheCommon.ApplyInstancePrefixToUri("prod", uri);
        result.Should().Be(uri);
    }
}
```

### 2. CommandClient Tests

**File:** `Unit/Services/CommandClientTests.cs`

```csharp
public class CommandClientTests
{
    private readonly Mock<IRequestClient<TestRequest>> _requestClientMock;
    private readonly Mock<ILogger<CommandClient<TestRequest>>> _loggerMock;
    private readonly CommandClient<TestRequest> _sut;

    public CommandClientTests()
    {
        _requestClientMock = new Mock<IRequestClient<TestRequest>>();
        _loggerMock = new Mock<ILogger<CommandClient<TestRequest>>>();
        _sut = new CommandClient<TestRequest>(_requestClientMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task GetResponse_Success_ReturnsResponseMessage()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        var expectedResponse = new TestResponse { Result = "success" };
        var responseMock = new Mock<Response<TestResponse>>();
        responseMock.Setup(r => r.Message).Returns(expectedResponse);

        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .ReturnsAsync(responseMock.Object);

        // Act
        var result = await _sut.GetResponse<TestResponse>(request);

        // Assert
        result.Should().BeEquivalentTo(expectedResponse);
    }

    [Fact]
    public async Task GetResponse_Timeout_ThrowsDistributionTimeoutException()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .ThrowsAsync(new RequestTimeoutException("Timeout"));

        // Act & Assert
        await _sut.Invoking(s => s.GetResponse<TestResponse>(request))
            .Should().ThrowAsync<DistributionTimeoutException>();
    }

    [Fact]
    public async Task GetResponse_GeneralException_ThrowsDistributedOperationFailedException()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .ThrowsAsync(new InvalidOperationException("Some error"));

        // Act & Assert
        await _sut.Invoking(s => s.GetResponse<TestResponse>(request))
            .Should().ThrowAsync<DistributedOperationFailedException>();
    }

    [Fact]
    public async Task GetResponseWithRetry_SuccessOnFirstAttempt_ReturnsImmediately()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        var expectedResponse = new TestResponse { Result = "success" };
        var responseMock = new Mock<Response<TestResponse>>();
        responseMock.Setup(r => r.Message).Returns(expectedResponse);

        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .ReturnsAsync(responseMock.Object);

        // Act
        var result = await _sut.GetResponseWithRetry<TestResponse>(request, retryCount: 3);

        // Assert
        result.Should().BeEquivalentTo(expectedResponse);
        _requestClientMock.Verify(
            c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()),
            Times.Once);
    }

    [Fact]
    public async Task GetResponseWithRetry_SuccessOnThirdAttempt_RetriesCorrectly()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        var expectedResponse = new TestResponse { Result = "success" };
        var responseMock = new Mock<Response<TestResponse>>();
        responseMock.Setup(r => r.Message).Returns(expectedResponse);

        var callCount = 0;
        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount < 3)
                    throw new RequestTimeoutException("Timeout");
                return Task.FromResult(responseMock.Object);
            });

        // Act
        var result = await _sut.GetResponseWithRetry<TestResponse>(request, retryCount: 5);

        // Assert
        result.Should().BeEquivalentTo(expectedResponse);
        callCount.Should().Be(3);
    }

    [Fact]
    public async Task GetResponseWithRetry_AllAttemptsFail_ThrowsAfterRetries()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .ThrowsAsync(new RequestTimeoutException("Timeout"));

        // Act & Assert
        await _sut.Invoking(s => s.GetResponseWithRetry<TestResponse>(request, retryCount: 3))
            .Should().ThrowAsync<DistributedOperationFailedException>()
            .WithMessage("*failed after 3 retries*");
    }
}

// Test DTOs
public record TestRequest { public string Value { get; init; } = null!; }
public record TestResponse { public string Result { get; init; } = null!; }
```

### 3. DistributedCacheService Tests

**File:** `Unit/Services/DistributedCacheServiceTests.cs`

```csharp
public class DistributedCacheServiceTests
{
    private readonly Mock<IRepositoryClient> _repositoryClientMock;
    private readonly Mock<ITenantResolver> _tenantResolverMock;
    private readonly Mock<IRepository> _repositoryMock;
    private readonly DistributedCacheService _sut;

    public DistributedCacheServiceTests()
    {
        _repositoryClientMock = new Mock<IRepositoryClient>();
        _tenantResolverMock = new Mock<ITenantResolver>();
        _repositoryMock = new Mock<IRepository>();
        _sut = new DistributedCacheService(_repositoryClientMock.Object, _tenantResolverMock.Object);

        // Default setup
        _tenantResolverMock
            .Setup(r => r.GetRepositoryNameAsync(It.IsAny<string>()))
            .ReturnsAsync("test-repo");
        _repositoryClientMock
            .Setup(c => c.GetRepositoryAsync("test-repo"))
            .ReturnsAsync(_repositoryMock.Object);
    }

    [Fact]
    public async Task CreateStreamAsync_UploadsToCorrectRepository()
    {
        // Arrange
        var tenantId = "tenant-1";
        var stream = new MemoryStream(new byte[] { 1, 2, 3 });
        var contentType = "application/pdf";
        var fileName = "test.pdf";
        var expiry = TimeSpan.FromHours(24);
        var expectedKey = "cache-key-123";

        _repositoryMock
            .Setup(r => r.UploadBinaryAsync(stream, contentType, fileName, It.IsAny<DateTime?>()))
            .ReturnsAsync(expectedKey);

        // Act
        var result = await _sut.CreateStreamAsync(tenantId, stream, contentType, fileName, expiry);

        // Assert
        result.Should().Be(expectedKey);
        _tenantResolverMock.Verify(r => r.GetRepositoryNameAsync(tenantId), Times.Once);
        _repositoryMock.Verify(r => r.UploadBinaryAsync(stream, contentType, fileName, It.IsAny<DateTime?>()), Times.Once);
    }

    [Fact]
    public async Task GetCacheStreamByIdAsync_FileExists_ReturnsCacheStream()
    {
        // Arrange
        var tenantId = "tenant-1";
        var cacheKey = "cache-key-123";
        var expectedStream = new MemoryStream(new byte[] { 1, 2, 3 });
        var downloadInfo = new Mock<IDownloadStreamHandler>();
        downloadInfo.Setup(d => d.ContentType).Returns("application/pdf");
        downloadInfo.Setup(d => d.Stream).Returns(expectedStream);
        downloadInfo.Setup(d => d.Filename).Returns("test.pdf");

        _repositoryMock
            .Setup(r => r.DownloadBinaryAsync(cacheKey))
            .ReturnsAsync(downloadInfo.Object);

        // Act
        var result = await _sut.GetCacheStreamByIdAsync(tenantId, cacheKey);

        // Assert
        result.Should().NotBeNull();
        result!.ContentType.Should().Be("application/pdf");
        result.FileName.Should().Be("test.pdf");
        result.Stream.Should().BeSameAs(expectedStream);
    }

    [Fact]
    public async Task GetCacheStreamByIdAsync_FileNotFound_ReturnsNull()
    {
        // Arrange
        var tenantId = "tenant-1";
        var cacheKey = "non-existent-key";

        _repositoryMock
            .Setup(r => r.DownloadBinaryAsync(cacheKey))
            .ReturnsAsync((IDownloadStreamHandler?)null);

        // Act
        var result = await _sut.GetCacheStreamByIdAsync(tenantId, cacheKey);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task DeleteCacheStreamAsync_CallsRepositoryDelete()
    {
        // Arrange
        var tenantId = "tenant-1";
        var cacheKey = "cache-key-123";

        // Act
        await _sut.DeleteCacheStreamAsync(tenantId, cacheKey);

        // Assert
        _repositoryMock.Verify(r => r.DeleteBinaryAsync(cacheKey), Times.Once);
    }

    [Fact]
    public async Task DeleteAllExpiredCacheStreamsAsync_CallsRepositoryWithCurrentTime()
    {
        // Arrange
        var tenantId = "tenant-1";

        // Act
        await _sut.DeleteAllExpiredCacheStreamsAsync(tenantId);

        // Assert
        _repositoryMock.Verify(r => r.DeleteAllExpiredBinariesAsync(It.IsAny<DateTime>()), Times.Once);
    }

    [Fact]
    public void IsConnected_ReturnsRepositoryClientState()
    {
        // Arrange
        _repositoryClientMock.Setup(c => c.IsConnected).Returns(true);

        // Act & Assert
        _sut.IsConnected.Should().BeTrue();

        _repositoryClientMock.Setup(c => c.IsConnected).Returns(false);
        _sut.IsConnected.Should().BeFalse();
    }
}
```

### 4. DistributionEventHubService Tests

**File:** `Unit/Services/DistributionEventHubServiceTests.cs`

```csharp
public class DistributionEventHubServiceTests
{
    private readonly Mock<IBus> _busMock;
    private readonly Mock<IBroadcastServiceAddress> _serviceAddressMock;
    private readonly Mock<ISendEndpoint> _sendEndpointMock;
    private readonly DistributionEventHubService _sut;

    public DistributionEventHubServiceTests()
    {
        _busMock = new Mock<IBus>();
        _serviceAddressMock = new Mock<IBroadcastServiceAddress>();
        _sendEndpointMock = new Mock<ISendEndpoint>();

        _serviceAddressMock.Setup(s => s.InstancePrefix).Returns("test");
        _busMock.Setup(b => b.GetSendEndpoint(It.IsAny<Uri>()))
            .ReturnsAsync(_sendEndpointMock.Object);

        _sut = new DistributionEventHubService(_busMock.Object, _serviceAddressMock.Object);
    }

    [Fact]
    public async Task SendAsync_AppliesInstancePrefixToAddress()
    {
        // Arrange
        var message = new TestMessage { Data = "test" };
        var address = new Uri("queue:my-queue");
        Uri? capturedUri = null;

        _busMock
            .Setup(b => b.GetSendEndpoint(It.IsAny<Uri>()))
            .Callback<Uri>(uri => capturedUri = uri)
            .ReturnsAsync(_sendEndpointMock.Object);

        // Act
        await _sut.SendAsync(address, message);

        // Assert
        capturedUri.Should().NotBeNull();
        capturedUri!.ToString().Should().Contain("test-");
    }

    [Fact]
    public async Task SendAsync_SendsMessageToEndpoint()
    {
        // Arrange
        var message = new TestMessage { Data = "test" };
        var address = new Uri("queue:my-queue");

        // Act
        await _sut.SendAsync(address, message);

        // Assert
        _sendEndpointMock.Verify(
            e => e.Send(message, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task CancelScheduledRecurringSendAsync_CallsBusCancel()
    {
        // Arrange
        var scheduleId = "test-schedule";
        var scheduleGroup = "test-group";

        // Act
        await _sut.CancelScheduledRecurringSendAsync(scheduleId, scheduleGroup);

        // Assert
        _busMock.Verify(b => b.CancelScheduledRecurringSend(scheduleId, scheduleGroup), Times.Once);
    }
}

public record TestMessage { public string Data { get; init; } = null!; }
```

### 5. DistributedConsumer Tests

**File:** `Unit/Consumers/DistributedConsumerTests.cs`

```csharp
public class DistributedConsumerTests
{
    [Fact]
    public async Task Consume_DelegatesToUserConsumer()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<DistributedConsumer<TestConsumer, TestEvent>>>();
        var loggerFactoryMock = new Mock<ILoggerFactory>();
        var eventHubMock = new Mock<IDistributionEventHubService>();
        var consumerMock = new Mock<TestConsumer>();
        var contextMock = new Mock<ConsumeContext<TestEvent>>();

        loggerFactoryMock
            .Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());

        var testEvent = new TestEvent { Id = "123" };
        contextMock.Setup(c => c.Message).Returns(testEvent);

        var sut = new DistributedConsumer<TestConsumer, TestEvent>(
            loggerMock.Object,
            loggerFactoryMock.Object,
            eventHubMock.Object,
            consumerMock.Object);

        // Act
        await sut.Consume(contextMock.Object);

        // Assert
        consumerMock.Verify(
            c => c.ConsumeAsync(It.Is<IDistributedContext<TestEvent>>(ctx => ctx.Message == testEvent)),
            Times.Once);
    }
}

public record TestEvent { public string Id { get; init; } = null!; }

public class TestConsumer : IDistributedConsumer<TestEvent>
{
    public virtual Task ConsumeAsync(IDistributedContext<TestEvent> context)
    {
        return Task.CompletedTask;
    }
}
```

### 6. Configuration Tests

**File:** `Unit/Configuration/DistributionEventHubConfigurationTests.cs`

```csharp
public class DistributionEventHubConfigurationTests
{
    [Fact]
    public void AddDistributionEventHub_NoUniqueServiceAddress_ThrowsException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<DistributionEventHubOptions>(o => o.InstancePrefix = "test");

        // Act & Assert
        var action = () => services.AddDistributionEventHub(config =>
        {
            // UniqueServiceAddress not set
        });

        action.Should().Throw<DistributedOperationFailedException>()
            .WithMessage("*UniqueServiceAddress*");
    }

    [Fact]
    public void AddDistributionEventHub_NoInstancePrefix_ThrowsException()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act & Assert
        var action = () => services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "TestService";
            config.InstancePrefix = "";  // Empty prefix
        });

        action.Should().Throw<DistributedOperationFailedException>()
            .WithMessage("*InstancePrefix*");
    }

    [Fact]
    public void AddDistributionEventHub_ValidConfiguration_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "test";
            o.BrokerHost = "localhost";
        });

        // Act
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "TestService";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        provider.GetService<IDistributionEventHubService>().Should().NotBeNull();
        provider.GetService<IBroadcastServiceAddress>().Should().NotBeNull();
    }

    [Fact]
    public void InstancePrefix_FromOptions_IsApplied()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "from-options";
            o.BrokerHost = "localhost";
        });

        // Act
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "TestService";
            // Don't set InstancePrefix here - should use from options
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var serviceAddress = provider.GetService<IBroadcastServiceAddress>();
        serviceAddress!.InstancePrefix.Should().Be("from-options");
    }

    [Fact]
    public void InstancePrefix_FromCode_OverridesOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "from-options";
            o.BrokerHost = "localhost";
        });

        // Act
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "TestService";
            config.InstancePrefix = "from-code";  // Override
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var serviceAddress = provider.GetService<IBroadcastServiceAddress>();
        serviceAddress!.InstancePrefix.Should().Be("from-code");
    }
}
```

### 7. Exception Tests

**File:** `Unit/Exceptions/DistributionTimeoutExceptionTests.cs`

```csharp
public class DistributionTimeoutExceptionTests
{
    [Fact]
    public void Create_IncludesCommandNameAndTimeout()
    {
        // Arrange
        var commandName = "GetUserCommand";
        var timeout = TimeSpan.FromSeconds(30);
        var innerException = new Exception("Inner");

        // Act
        var exception = DistributionTimeoutException.Create(commandName, timeout, innerException);

        // Assert
        exception.Message.Should().Contain(commandName);
        exception.Message.Should().Contain("30");
        exception.InnerException.Should().BeSameAs(innerException);
    }
}

public class DistributedOperationFailedExceptionTests
{
    [Fact]
    public void CreateCommandFailed_IncludesCommandName()
    {
        // Arrange
        var commandName = "ProcessDataCommand";
        var innerException = new Exception("Processing failed");

        // Act
        var exception = DistributedOperationFailedException.CreateCommandFailed(commandName, innerException);

        // Assert
        exception.Message.Should().Contain(commandName);
        exception.Message.Should().Contain("failed");
        exception.InnerException.Should().BeSameAs(innerException);
    }

    [Fact]
    public void NoUniqueServiceAddress_ReturnsDescriptiveMessage()
    {
        // Act
        var exception = DistributedOperationFailedException.NoUniqueServiceAddress();

        // Assert
        exception.Message.Should().Contain("UniqueServiceAddress");
    }

    [Fact]
    public void NoInstancePrefix_ReturnsDescriptiveMessage()
    {
        // Act
        var exception = DistributedOperationFailedException.NoInstancePrefix();

        // Assert
        exception.Message.Should().Contain("InstancePrefix");
    }
}
```

---

## Integration Tests

### 1. Test Fixtures

**File:** `Integration/Fixtures/RabbitMqFixture.cs`

```csharp
public class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container;

    public string ConnectionString => _container.GetConnectionString();
    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(5672);

    public RabbitMqFixture()
    {
        _container = new RabbitMqBuilder()
            .WithImage("rabbitmq:3-management")
            .WithUsername("guest")
            .WithPassword("guest")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("RabbitMQ")]
public class RabbitMqCollection : ICollectionFixture<RabbitMqFixture> { }
```

**File:** `Integration/Fixtures/MongoDbFixture.cs`

```csharp
public class MongoDbFixture : IAsyncLifetime
{
    private readonly MongoDbContainer _container;

    public string ConnectionString => _container.GetConnectionString();

    public MongoDbFixture()
    {
        _container = new MongoDbBuilder()
            .WithImage("mongo:7")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("MongoDB")]
public class MongoDbCollection : ICollectionFixture<MongoDbFixture> { }
```

### 2. Broadcast Event Integration Tests

**File:** `Integration/Messaging/BroadcastEventTests.cs`

```csharp
[Collection("RabbitMQ")]
public class BroadcastEventTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _serviceProvider = null!;
    private readonly List<ReceivedMessage> _receivedMessages = new();

    public BroadcastEventTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "integration-test";
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = "guest";
            o.BrokerPassword = "guest";
        });

        services.AddSingleton(_receivedMessages);
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "BroadcastTestService";
            config.AddBroadcastEventConsumer<TestBroadcastConsumer, TestBroadcastMessage>();
        });

        _serviceProvider = services.BuildServiceProvider();

        // Wait for bus to start
        await Task.Delay(2000);
    }

    public async Task DisposeAsync()
    {
        await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task PublishAsync_BroadcastMessage_IsReceivedByConsumer()
    {
        // Arrange
        var eventHub = _serviceProvider.GetRequiredService<IDistributionEventHubService>();
        var message = new TestBroadcastMessage { Id = Guid.NewGuid().ToString(), Content = "Test broadcast" };

        // Act
        await eventHub.PublishAsync(message);

        // Assert - Wait for message to be consumed
        await WaitForCondition(() => _receivedMessages.Any(m => m.Id == message.Id), TimeSpan.FromSeconds(10));

        _receivedMessages.Should().ContainSingle(m => m.Id == message.Id);
    }

    [Fact]
    public async Task PublishAsync_MultipleMessages_AllAreReceived()
    {
        // Arrange
        var eventHub = _serviceProvider.GetRequiredService<IDistributionEventHubService>();
        var messages = Enumerable.Range(1, 5)
            .Select(i => new TestBroadcastMessage { Id = $"msg-{i}", Content = $"Message {i}" })
            .ToList();

        // Act
        foreach (var message in messages)
        {
            await eventHub.PublishAsync(message);
        }

        // Assert
        await WaitForCondition(() => _receivedMessages.Count >= 5, TimeSpan.FromSeconds(15));

        foreach (var message in messages)
        {
            _receivedMessages.Should().Contain(m => m.Id == message.Id);
        }
    }

    private static async Task WaitForCondition(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
        }
    }
}

public record TestBroadcastMessage
{
    public string Id { get; init; } = null!;
    public string Content { get; init; } = null!;
}

public record ReceivedMessage(string Id, DateTime ReceivedAt);

public class TestBroadcastConsumer : IDistributedConsumer<TestBroadcastMessage>
{
    private readonly List<ReceivedMessage> _receivedMessages;

    public TestBroadcastConsumer(List<ReceivedMessage> receivedMessages)
    {
        _receivedMessages = receivedMessages;
    }

    public Task ConsumeAsync(IDistributedContext<TestBroadcastMessage> context)
    {
        _receivedMessages.Add(new ReceivedMessage(context.Message.Id, DateTime.UtcNow));
        return Task.CompletedTask;
    }
}
```

### 3. Command Client Integration Tests

**File:** `Integration/Messaging/CommandClientIntegrationTests.cs`

```csharp
[Collection("RabbitMQ")]
public class CommandClientIntegrationTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _consumerServiceProvider = null!;
    private ServiceProvider _clientServiceProvider = null!;

    public CommandClientIntegrationTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async Task InitializeAsync()
    {
        // Setup Consumer Service
        var consumerServices = new ServiceCollection();
        consumerServices.AddLogging();
        consumerServices.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "cmd-test";
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = "guest";
            o.BrokerPassword = "guest";
        });

        consumerServices.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "CommandConsumerService";
            config.AddCommandConsumer<EchoCommandConsumer, EchoRequest>("echo-command");
        });

        _consumerServiceProvider = consumerServices.BuildServiceProvider();

        // Setup Client Service
        var clientServices = new ServiceCollection();
        clientServices.AddLogging();
        clientServices.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "cmd-test";
            o.BrokerHost = _rabbitMq.Host;
            o.BrokerPort = (ushort)_rabbitMq.Port;
            o.BrokerUser = "guest";
            o.BrokerPassword = "guest";
        });

        clientServices.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "CommandClientService";
            config.AddCommandClient<EchoRequest>("echo-command", TimeSpan.FromSeconds(30));
        });

        _clientServiceProvider = clientServices.BuildServiceProvider();

        // Wait for services to start
        await Task.Delay(3000);
    }

    public async Task DisposeAsync()
    {
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
        var tasks = Enumerable.Range(1, 10)
            .Select(i => client.GetResponse<EchoResponse>(new EchoRequest { Message = $"Message {i}" }))
            .ToList();

        // Act
        var responses = await Task.WhenAll(tasks);

        // Assert
        responses.Should().HaveCount(10);
        responses.Select(r => r.Echo).Should().OnlyContain(e => e.StartsWith("Echo:"));
    }
}

public record EchoRequest { public string Message { get; init; } = null!; }
public record EchoResponse { public string Echo { get; init; } = null!; }

public class EchoCommandConsumer : IDistributedConsumer<EchoRequest>
{
    public async Task ConsumeAsync(IDistributedContext<EchoRequest> context)
    {
        await context.RespondAsync(new EchoResponse { Echo = $"Echo: {context.Message.Message}" });
    }
}
```

### 4. MongoDB Caching Integration Tests

**File:** `Integration/Caching/MongoDbCachingTests.cs`

```csharp
[Collection("MongoDB")]
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
            o.InstancePrefix = "cache-test";
            o.RepositoryHost = _mongoDb.ConnectionString;
            o.SystemDatabaseName = "TestCache";
        });

        services.AddSingleton<ITenantResolver, TestTenantResolver>();
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "CacheTestService";
        }).AddMongoDbRepository();

        _serviceProvider = services.BuildServiceProvider();
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

        // Act
        var cacheKey = await cacheService.CreateStreamAsync(
            "tenant-1", stream, "text/plain", "test.txt", TimeSpan.FromHours(1));

        var retrieved = await cacheService.GetCacheStreamByIdAsync("tenant-1", cacheKey);

        // Assert
        cacheKey.Should().NotBeNullOrEmpty();
        retrieved.Should().NotBeNull();
        retrieved!.ContentType.Should().Be("text/plain");
        retrieved.FileName.Should().Be("test.txt");

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
        var cacheKey = await cacheService.CreateStreamAsync(
            "tenant-1", stream, "text/plain", "delete-test.txt");

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
        var stream = new MemoryStream("Find me by name"u8.ToArray());
        await cacheService.CreateStreamAsync(
            "tenant-1", stream, "text/plain", "findable.txt");

        // Act
        var retrieved = await cacheService.GetCacheStreamByFileNameAsync("tenant-1", "findable.txt");

        // Assert
        retrieved.Should().NotBeNull();
        retrieved!.FileName.Should().Be("findable.txt");
    }

    [Fact]
    public async Task CreateOrUpdateStream_UpdatesExistingFile()
    {
        // Arrange
        var cacheService = _serviceProvider.GetRequiredService<IDistributedCacheService>();
        var originalStream = new MemoryStream("Original content"u8.ToArray());
        var updatedStream = new MemoryStream("Updated content"u8.ToArray());

        // Act
        await cacheService.CreateOrUpdateStreamAsync("tenant-1", originalStream, "text/plain", "updateable.txt");
        await cacheService.CreateOrUpdateStreamAsync("tenant-1", updatedStream, "text/plain", "updateable.txt");

        var retrieved = await cacheService.GetCacheStreamByFileNameAsync("tenant-1", "updateable.txt");

        // Assert
        retrieved.Should().NotBeNull();
        using var reader = new StreamReader(retrieved!.Stream);
        var content = await reader.ReadToEndAsync();
        content.Should().Be("Updated content");
    }
}

public class TestTenantResolver : ITenantResolver
{
    public Task<string> GetRepositoryNameAsync(string tenantId)
    {
        return Task.FromResult($"cache-{tenantId}");
    }
}
```

### 5. Instance Isolation Integration Tests

**File:** `Integration/Messaging/InstanceIsolationTests.cs`

```csharp
[Collection("RabbitMQ")]
public class InstanceIsolationTests : IAsyncLifetime
{
    private readonly RabbitMqFixture _rabbitMq;
    private ServiceProvider _instance1Provider = null!;
    private ServiceProvider _instance2Provider = null!;
    private readonly List<(string Instance, string MessageId)> _receivedMessages = new();

    public InstanceIsolationTests(RabbitMqFixture rabbitMq)
    {
        _rabbitMq = rabbitMq;
    }

    public async Task InitializeAsync()
    {
        // Instance 1 with prefix "instance-a"
        _instance1Provider = CreateServiceProvider("instance-a", "Instance1");

        // Instance 2 with prefix "instance-b"
        _instance2Provider = CreateServiceProvider("instance-b", "Instance2");

        await Task.Delay(3000);
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
            o.BrokerUser = "guest";
            o.BrokerPassword = "guest";
        });

        services.AddSingleton(_receivedMessages);
        services.AddSingleton(instancePrefix); // For consumer identification
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = serviceName;
            config.AddBroadcastEventConsumer<IsolationTestConsumer, IsolationTestMessage>();
        });

        return services.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        await _instance1Provider.DisposeAsync();
        await _instance2Provider.DisposeAsync();
    }

    [Fact]
    public async Task Publish_DifferentInstances_MessagesAreIsolated()
    {
        // Arrange
        var eventHub1 = _instance1Provider.GetRequiredService<IDistributionEventHubService>();
        var eventHub2 = _instance2Provider.GetRequiredService<IDistributionEventHubService>();

        var message1 = new IsolationTestMessage { Id = "from-instance-1" };
        var message2 = new IsolationTestMessage { Id = "from-instance-2" };

        // Act
        await eventHub1.PublishAsync(message1);
        await eventHub2.PublishAsync(message2);

        // Wait for messages
        await Task.Delay(5000);

        // Assert - Each instance should only receive its own message
        var instance1Messages = _receivedMessages.Where(m => m.Instance == "instance-a").ToList();
        var instance2Messages = _receivedMessages.Where(m => m.Instance == "instance-b").ToList();

        instance1Messages.Should().ContainSingle(m => m.MessageId == "from-instance-1");
        instance1Messages.Should().NotContain(m => m.MessageId == "from-instance-2");

        instance2Messages.Should().ContainSingle(m => m.MessageId == "from-instance-2");
        instance2Messages.Should().NotContain(m => m.MessageId == "from-instance-1");
    }
}

public record IsolationTestMessage { public string Id { get; init; } = null!; }

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
```

---

## Test Infrastructure

### Test Project File

**File:** `tests/DistributionEventHub.Tests/DistributionEventHub.Tests.csproj`

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.*" />
    <PackageReference Include="xunit" Version="2.*" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.*" />
    <PackageReference Include="Moq" Version="4.*" />
    <PackageReference Include="FluentAssertions" Version="6.*" />
    <PackageReference Include="Testcontainers.RabbitMq" Version="3.*" />
    <PackageReference Include="Testcontainers.MongoDb" Version="3.*" />
    <PackageReference Include="MassTransit.Testing" Version="8.5.7" />
    <PackageReference Include="coverlet.collector" Version="6.*" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\..\src\DistributionEventHub\DistributionEventHub.csproj" />
    <ProjectReference Include="..\..\src\DistributionEventHub.MongoDB\DistributionEventHub.MongoDB.csproj" />
  </ItemGroup>

</Project>
```

### Test Settings

**File:** `tests/DistributionEventHub.Tests/xunit.runner.json`

```json
{
  "parallelizeAssembly": false,
  "parallelizeTestCollections": false,
  "maxParallelThreads": 1
}
```

---

## Test Data and Fixtures

### Shared Test Utilities

**File:** `Integration/Fixtures/TestHelpers.cs`

```csharp
public static class TestHelpers
{
    public static async Task WaitForCondition(
        Func<bool> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(interval);
        }

        if (!condition())
        {
            throw new TimeoutException($"Condition not met within {timeout.TotalSeconds} seconds");
        }
    }

    public static async Task<T> WaitForResult<T>(
        Func<T?> getter,
        TimeSpan timeout) where T : class
    {
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            var result = getter();
            if (result != null) return result;
            await Task.Delay(100);
        }

        throw new TimeoutException($"Result not available within {timeout.TotalSeconds} seconds");
    }
}
```

---

## CI/CD Integration

### GitHub Actions Workflow

**File:** `.github/workflows/tests.yml`

```yaml
name: Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Restore dependencies
        run: dotnet restore

      - name: Build
        run: dotnet build --no-restore

      - name: Run Unit Tests
        run: dotnet test tests/DistributionEventHub.Tests --no-build --filter "Category!=Integration" --verbosity normal --collect:"XPlat Code Coverage"

      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          files: '**/coverage.cobertura.xml'

  integration-tests:
    runs-on: ubuntu-latest
    services:
      rabbitmq:
        image: rabbitmq:3-management
        ports:
          - 5672:5672
          - 15672:15672
      mongodb:
        image: mongo:7
        ports:
          - 27017:27017
    steps:
      - uses: actions/checkout@v4

      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Restore dependencies
        run: dotnet restore

      - name: Build
        run: dotnet build --no-restore

      - name: Run Integration Tests
        run: dotnet test tests/DistributionEventHub.Tests --no-build --filter "Category=Integration" --verbosity normal
        env:
          RABBITMQ_HOST: localhost
          MONGODB_HOST: localhost
```

### Test Categories

Use `[Trait]` attributes to categorize tests:

```csharp
// Unit test
[Fact]
[Trait("Category", "Unit")]
public void MyUnitTest() { }

// Integration test
[Fact]
[Trait("Category", "Integration")]
public async Task MyIntegrationTest() { }
```

---

## Test Execution Commands

```bash
# Run all tests
dotnet test

# Run only unit tests
dotnet test --filter "Category!=Integration"

# Run only integration tests
dotnet test --filter "Category=Integration"

# Run with coverage
dotnet test --collect:"XPlat Code Coverage"

# Run specific test class
dotnet test --filter "FullyQualifiedName~CommandClientTests"

# Run specific test method
dotnet test --filter "FullyQualifiedName~GetResponse_Success_ReturnsResponseMessage"
```
