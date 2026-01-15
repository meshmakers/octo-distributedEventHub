using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

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
    public async Task GetResponse_WithTimeout_UsesProvidedTimeout()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        var expectedResponse = new TestResponse { Result = "success" };
        var responseMock = new Mock<Response<TestResponse>>();
        responseMock.Setup(r => r.Message).Returns(expectedResponse);
        var timeout = TimeSpan.FromSeconds(60);

        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .ReturnsAsync(responseMock.Object);

        // Act
        var result = await _sut.GetResponse<TestResponse>(request, timeout: timeout);

        // Assert
        result.Should().BeEquivalentTo(expectedResponse);
        _requestClientMock.Verify(
            c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()),
            Times.Once);
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

    [Fact]
    public async Task GetResponseWithRetry_DefaultRetryCount_Is5()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        var callCount = 0;
        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, It.IsAny<CancellationToken>(), It.IsAny<RequestTimeout>()))
            .Returns(() =>
            {
                callCount++;
                throw new RequestTimeoutException("Timeout");
            });

        // Act & Assert
        await _sut.Invoking(s => s.GetResponseWithRetry<TestResponse>(request))
            .Should().ThrowAsync<DistributedOperationFailedException>();

        callCount.Should().Be(5);
    }

    [Fact]
    public async Task GetResponse_WithCancellationToken_PassesTokenToRequestClient()
    {
        // Arrange
        var request = new TestRequest { Value = "test" };
        var expectedResponse = new TestResponse { Result = "success" };
        var responseMock = new Mock<Response<TestResponse>>();
        responseMock.Setup(r => r.Message).Returns(expectedResponse);
        var cts = new CancellationTokenSource();

        _requestClientMock
            .Setup(c => c.GetResponse<TestResponse>(request, cts.Token, It.IsAny<RequestTimeout>()))
            .ReturnsAsync(responseMock.Object);

        // Act
        var result = await _sut.GetResponse<TestResponse>(request, cts.Token);

        // Assert
        result.Should().BeEquivalentTo(expectedResponse);
        _requestClientMock.Verify(
            c => c.GetResponse<TestResponse>(request, cts.Token, It.IsAny<RequestTimeout>()),
            Times.Once);
    }
}

// Test DTOs
public record TestRequest
{
    public string Value { get; init; } = null!;
}

public record TestResponse
{
    public string Result { get; init; } = null!;
}
