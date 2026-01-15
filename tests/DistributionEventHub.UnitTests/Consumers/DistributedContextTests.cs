using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Consumers;

[Trait("Category", "Unit")]
public class DistributedContextTests
{
    private readonly Mock<ILogger<DistributedContext<ContextTestMessage>>> _loggerMock;
    private readonly Mock<ConsumeContext<ContextTestMessage>> _consumeContextMock;
    private readonly Mock<IDistributionEventHubService> _eventHubMock;
    private readonly DistributedContext<ContextTestMessage> _sut;
    private readonly ContextTestMessage _testMessage;

    public DistributedContextTests()
    {
        _loggerMock = new Mock<ILogger<DistributedContext<ContextTestMessage>>>();
        _consumeContextMock = new Mock<ConsumeContext<ContextTestMessage>>();
        _eventHubMock = new Mock<IDistributionEventHubService>();

        _testMessage = new ContextTestMessage { Id = "test-123", Content = "Hello" };
        _consumeContextMock.Setup(c => c.Message).Returns(_testMessage);

        _sut = new DistributedContext<ContextTestMessage>(
            _loggerMock.Object,
            _consumeContextMock.Object,
            _eventHubMock.Object);
    }

    [Fact]
    public void Message_ReturnsContextMessage()
    {
        // Act
        var result = _sut.Message;

        // Assert
        result.Should().Be(_testMessage);
        result.Id.Should().Be("test-123");
        result.Content.Should().Be("Hello");
    }

    [Fact]
    public async Task RespondAsync_DelegatesToConsumeContext()
    {
        // Arrange
        var response = new ContextTestResponse { Result = "Success" };

        // Act
        await _sut.RespondAsync(response);

        // Assert
        _consumeContextMock.Verify(c => c.RespondAsync(response), Times.Once);
    }

    [Fact]
    public async Task RespondAsync_LogsResponseType()
    {
        // Arrange
        var response = new ContextTestResponse { Result = "Success" };

        // Act
        await _sut.RespondAsync(response);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => true),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_UsesEventHubService()
    {
        // Arrange
        var publishMessage = new ContextTestPublishMessage { Data = "Broadcast" };

        // Act
        await _sut.PublishAsync(publishMessage);

        // Assert
        _eventHubMock.Verify(e => e.PublishAsync(publishMessage, It.IsAny<CancellationToken?>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_LogsPublishType()
    {
        // Arrange
        var publishMessage = new ContextTestPublishMessage { Data = "Broadcast" };

        // Act
        await _sut.PublishAsync(publishMessage);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => true),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
            Times.Once);
    }

    [Fact]
    public async Task RespondAsync_WithDifferentResponseType_Works()
    {
        // Arrange
        var errorResponse = new ContextTestErrorResponse { ErrorCode = 500, ErrorMessage = "Internal Error" };

        // Act
        await _sut.RespondAsync(errorResponse);

        // Assert
        _consumeContextMock.Verify(c => c.RespondAsync(errorResponse), Times.Once);
    }
}

public record ContextTestMessage
{
    public string Id { get; init; } = null!;
    public string Content { get; init; } = null!;
}

public record ContextTestResponse
{
    public string Result { get; init; } = null!;
}

public record ContextTestErrorResponse
{
    public int ErrorCode { get; init; }
    public string ErrorMessage { get; init; } = null!;
}

public record ContextTestPublishMessage
{
    public string Data { get; init; } = null!;
}
