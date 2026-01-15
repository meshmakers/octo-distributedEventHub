using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Consumers;

[Trait("Category", "Unit")]
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

    [Fact]
    public async Task Consume_CreatesDistributedContextWithCorrectMessage()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<DistributedConsumer<CapturingConsumer, TestEvent>>>();
        var loggerFactoryMock = new Mock<ILoggerFactory>();
        var eventHubMock = new Mock<IDistributionEventHubService>();
        var contextMock = new Mock<ConsumeContext<TestEvent>>();

        loggerFactoryMock
            .Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());

        var testEvent = new TestEvent { Id = "event-456" };
        contextMock.Setup(c => c.Message).Returns(testEvent);

        IDistributedContext<TestEvent>? capturedContext = null;
        var consumer = new CapturingConsumer(ctx => capturedContext = ctx);

        var sut = new DistributedConsumer<CapturingConsumer, TestEvent>(
            loggerMock.Object,
            loggerFactoryMock.Object,
            eventHubMock.Object,
            consumer);

        // Act
        await sut.Consume(contextMock.Object);

        // Assert
        capturedContext.Should().NotBeNull();
        capturedContext!.Message.Should().Be(testEvent);
    }

    [Fact]
    public async Task Consume_LogsMessageType()
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

        // Assert - Verify logging was called
        loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => true),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
            Times.Once);
    }
}

public record TestEvent
{
    public string Id { get; init; } = null!;
}

public class TestConsumer : IDistributedConsumer<TestEvent>
{
    public virtual Task ConsumeAsync(IDistributedContext<TestEvent> context)
    {
        return Task.CompletedTask;
    }
}

public class CapturingConsumer : IDistributedConsumer<TestEvent>
{
    private readonly Action<IDistributedContext<TestEvent>> _captureAction;

    public CapturingConsumer(Action<IDistributedContext<TestEvent>> captureAction)
    {
        _captureAction = captureAction;
    }

    public Task ConsumeAsync(IDistributedContext<TestEvent> context)
    {
        _captureAction(context);
        return Task.CompletedTask;
    }
}
