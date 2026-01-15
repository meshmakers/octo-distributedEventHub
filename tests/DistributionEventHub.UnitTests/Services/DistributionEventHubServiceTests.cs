using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

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
    public async Task SendAsync_WithCancellationToken_PassesTokenToEndpoint()
    {
        // Arrange
        var message = new TestMessage { Data = "test" };
        var address = new Uri("queue:my-queue");
        var cts = new CancellationTokenSource();

        // Act
        await _sut.SendAsync(address, message, cts.Token);

        // Assert
        _sendEndpointMock.Verify(
            e => e.Send(message, cts.Token),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_WithExchangeAddress_AppliesPrefixCorrectly()
    {
        // Arrange
        var message = new TestMessage { Data = "test" };
        var address = new Uri("exchange:my-exchange?temporary=true");
        Uri? capturedUri = null;

        _busMock
            .Setup(b => b.GetSendEndpoint(It.IsAny<Uri>()))
            .Callback<Uri>(uri => capturedUri = uri)
            .ReturnsAsync(_sendEndpointMock.Object);

        // Act
        await _sut.SendAsync(address, message);

        // Assert
        capturedUri.Should().NotBeNull();
        capturedUri!.ToString().Should().StartWith("exchange:test-");
    }

    [Fact]
    public async Task SendAsync_WithQueueUri_AppliesCorrectPrefix()
    {
        // Arrange
        var message = new TestMessage { Data = "test" };
        var address = new Uri("queue:processor-queue");
        Uri? capturedUri = null;

        _busMock
            .Setup(b => b.GetSendEndpoint(It.IsAny<Uri>()))
            .Callback<Uri>(uri => capturedUri = uri)
            .ReturnsAsync(_sendEndpointMock.Object);

        // Act
        await _sut.SendAsync(address, message);

        // Assert
        capturedUri.Should().NotBeNull();
        capturedUri!.ToString().Should().Be("queue:test-processor-queue");
    }

    [Fact]
    public async Task SendAsync_ReturnsCompletedTask()
    {
        // Arrange
        var message = new TestMessage { Data = "test" };
        var address = new Uri("queue:my-queue");

        _sendEndpointMock
            .Setup(e => e.Send(message, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        var result = await _sut.SendAsync(address, message);

        // Assert
        result.Should().NotBeNull();
    }
}

public record TestMessage
{
    public string Data { get; init; } = null!;
}
