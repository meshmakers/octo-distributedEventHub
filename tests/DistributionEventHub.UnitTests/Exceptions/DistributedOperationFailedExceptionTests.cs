using FluentAssertions;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Exceptions;

[Trait("Category", "Unit")]
public class DistributedOperationFailedExceptionTests
{
    [Fact]
    public void DefaultConstructor_CreatesException()
    {
        // Act
        var exception = new DistributedOperationFailedException();

        // Assert
        exception.Should().NotBeNull();
        exception.Should().BeAssignableTo<DistributionException>();
    }

    [Fact]
    public void MessageConstructor_SetsMessage()
    {
        // Arrange
        var message = "Operation failed due to network error";

        // Act
        var exception = new DistributedOperationFailedException(message);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void MessageAndInnerExceptionConstructor_SetsBoth()
    {
        // Arrange
        var message = "Operation failed";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new DistributedOperationFailedException(message, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().BeSameAs(innerException);
    }

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
    public void CreateCommandFailed_MessageFormat()
    {
        // Arrange
        var commandName = "GetUserCommand";
        var innerException = new Exception("User not found");

        // Act
        var exception = DistributedOperationFailedException.CreateCommandFailed(commandName, innerException);

        // Assert
        exception.Message.Should().Be($"Command {commandName} failed.");
    }

    [Fact]
    public void NoUniqueServiceAddress_ReturnsDescriptiveMessage()
    {
        // Act
        var exception = DistributedOperationFailedException.NoUniqueServiceAddress();

        // Assert
        exception.Message.Should().Contain("UniqueServiceAddress");
        exception.Should().BeOfType<DistributedOperationFailedException>();
    }

    [Fact]
    public void NoInstancePrefix_ReturnsDescriptiveMessage()
    {
        // Act
        var exception = DistributedOperationFailedException.NoInstancePrefix();

        // Assert
        exception.Message.Should().Contain("InstancePrefix");
        exception.Should().BeOfType<DistributedOperationFailedException>();
    }

    [Fact]
    public void NoUniqueServiceAddress_SuggestsSettingProperty()
    {
        // Act
        var exception = DistributedOperationFailedException.NoUniqueServiceAddress();

        // Assert
        exception.Message.Should().Contain("set");
    }

    [Fact]
    public void NoInstancePrefix_SuggestsConfiguration()
    {
        // Act
        var exception = DistributedOperationFailedException.NoInstancePrefix();

        // Assert
        exception.Message.Should().Contain("configuration");
    }

    [Fact]
    public void Exception_IsDistributionException()
    {
        // Act
        var exception = new DistributedOperationFailedException("Test");

        // Assert
        exception.Should().BeAssignableTo<DistributionException>();
    }
}
