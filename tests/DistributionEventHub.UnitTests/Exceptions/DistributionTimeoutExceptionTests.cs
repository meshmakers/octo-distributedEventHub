using FluentAssertions;
using MassTransit;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Exceptions;

[Trait("Category", "Unit")]
public class DistributionTimeoutExceptionTests
{
    [Fact]
    public void Create_IncludesCommandNameAndTimeout()
    {
        // Arrange
        var commandName = "GetUserCommand";
        var timeout = RequestTimeout.After(s: 30);
        var innerException = new Exception("Inner");

        // Act
        var exception = DistributionTimeoutException.Create(commandName, timeout, innerException);

        // Assert
        exception.Message.Should().Contain(commandName);
        exception.Message.Should().Contain("30");
        exception.InnerException.Should().BeSameAs(innerException);
    }

    [Fact]
    public void Create_WithDifferentTimeout_ShowsCorrectSeconds()
    {
        // Arrange
        var commandName = "ProcessDataCommand";
        var timeout = RequestTimeout.After(s: 60);
        var innerException = new Exception("Timeout occurred");

        // Act
        var exception = DistributionTimeoutException.Create(commandName, timeout, innerException);

        // Assert
        exception.Message.Should().Contain("60");
    }

    [Fact]
    public void Create_MessageContainsTimedOut()
    {
        // Arrange
        var commandName = "TestCommand";
        var timeout = RequestTimeout.After(s: 10);
        var innerException = new Exception("Inner");

        // Act
        var exception = DistributionTimeoutException.Create(commandName, timeout, innerException);

        // Assert
        exception.Message.Should().Contain("timed out");
    }

    [Fact]
    public void Exception_IsDistributionException()
    {
        // Arrange
        var commandName = "TestCommand";
        var timeout = RequestTimeout.After(s: 10);
        var innerException = new Exception("Inner");

        // Act
        var exception = DistributionTimeoutException.Create(commandName, timeout, innerException);

        // Assert
        exception.Should().BeAssignableTo<DistributionException>();
    }

    [Fact]
    public void Create_WithMinutes_ShowsCorrectSeconds()
    {
        // Arrange
        var commandName = "LongRunningCommand";
        var timeout = RequestTimeout.After(m: 2); // 2 minutes = 120 seconds
        var innerException = new Exception("Timeout");

        // Act
        var exception = DistributionTimeoutException.Create(commandName, timeout, innerException);

        // Assert
        exception.Message.Should().Contain("120");
    }
}
