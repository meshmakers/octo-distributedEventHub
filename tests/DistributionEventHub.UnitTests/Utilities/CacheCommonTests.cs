using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Utilities;

public class CacheCommonTests
{
    [Theory]
    [InlineData("prod", "prod-")]
    [InlineData("STAGING", "staging-")]
    [InlineData("Dev", "dev-")]
    [InlineData("TEST", "test-")]
    public void GetInstancePrefix_ReturnsLowercasedPrefixWithDash(string input, string expected)
    {
        // Act
        var result = CacheCommon.GetInstancePrefix(input);

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("prod", "my-queue", "prod-my-queue")]
    [InlineData("staging", "processor", "staging-processor")]
    [InlineData("DEV", "test-queue", "dev-test-queue")]
    public void ApplyInstancePrefix_CombinesPrefixAndName(string prefix, string name, string expected)
    {
        // Act
        var result = CacheCommon.ApplyInstancePrefix(prefix, name);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void ApplyInstancePrefixToUri_Queue_PrefixesCorrectly()
    {
        // Arrange
        var uri = new Uri("queue:my-queue");

        // Act
        var result = CacheCommon.ApplyInstancePrefixToUri("prod", uri);

        // Assert
        result.ToString().Should().Be("queue:prod-my-queue");
    }

    [Fact]
    public void ApplyInstancePrefixToUri_Exchange_PrefixesCorrectly()
    {
        // Arrange
        var uri = new Uri("exchange:my-exchange?temporary=true");

        // Act
        var result = CacheCommon.ApplyInstancePrefixToUri("prod", uri);

        // Assert
        result.ToString().Should().Be("exchange:prod-my-exchange?temporary=true");
    }

    [Fact]
    public void ApplyInstancePrefixToUri_EmptyPrefix_ReturnsOriginal()
    {
        // Arrange
        var uri = new Uri("queue:my-queue");

        // Act
        var result = CacheCommon.ApplyInstancePrefixToUri("", uri);

        // Assert
        result.Should().Be(uri);
    }

    [Fact]
    public void ApplyInstancePrefixToUri_WhitespacePrefix_ReturnsOriginal()
    {
        // Arrange
        var uri = new Uri("queue:my-queue");

        // Act
        var result = CacheCommon.ApplyInstancePrefixToUri("   ", uri);

        // Assert
        result.Should().Be(uri);
    }

    [Fact]
    public void ApplyInstancePrefixToUri_NullPrefix_ReturnsOriginal()
    {
        // Arrange
        var uri = new Uri("queue:my-queue");

        // Act
        var result = CacheCommon.ApplyInstancePrefixToUri(null!, uri);

        // Assert
        result.Should().Be(uri);
    }

    [Fact]
    public void ApplyInstancePrefixToUri_UnknownScheme_ReturnsOriginal()
    {
        // Arrange
        var uri = new Uri("http://example.com");

        // Act
        var result = CacheCommon.ApplyInstancePrefixToUri("prod", uri);

        // Assert
        result.Should().Be(uri);
    }

    [Fact]
    public void ApplyInstancePrefixToUri_ExchangeWithoutQueryString_PrefixesCorrectly()
    {
        // Arrange
        var uri = new Uri("exchange:my-exchange");

        // Act
        var result = CacheCommon.ApplyInstancePrefixToUri("staging", uri);

        // Assert
        result.ToString().Should().Be("exchange:staging-my-exchange");
    }

    [Fact]
    public void ContentType_HasExpectedValue()
    {
        // Assert
        CacheCommon.ContentType.Should().Be("contentType");
    }

    [Fact]
    public void ExpiryDateTime_HasExpectedValue()
    {
        // Assert
        CacheCommon.ExpiryDateTime.Should().Be("expiryDateTime");
    }
}
