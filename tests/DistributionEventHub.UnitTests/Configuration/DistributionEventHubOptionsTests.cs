using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Configuration;

[Trait("Category", "Unit")]
public class DistributionEventHubOptionsTests
{
    [Fact]
    public void DefaultValues_AreCorrect()
    {
        // Act
        var options = new DistributionEventHubOptions();

        // Assert
        options.BrokerHost.Should().Be("localhost");
        options.BrokerPort.Should().Be(5672);
        options.RepositoryHost.Should().Be("localhost");
        options.SystemDatabaseName.Should().Be("OctoSystem");
        options.RepositoryUser.Should().Be("octo-system-admin");
        options.RepositoryUseTls.Should().BeTrue();
        options.RepositoryAllowInsecureTls.Should().BeFalse();
        options.DatabaseAuthenticationSource.Should().Be("admin");
    }

    [Fact]
    public void InstancePrefix_CanBeSet()
    {
        // Arrange
        var options = new DistributionEventHubOptions();

        // Act
        options.InstancePrefix = "production";

        // Assert
        options.InstancePrefix.Should().Be("production");
    }

    [Fact]
    public void BrokerCredentials_CanBeSet()
    {
        // Arrange
        var options = new DistributionEventHubOptions();

        // Act
        options.BrokerUser = "admin";
        options.BrokerPassword = "secret";

        // Assert
        options.BrokerUser.Should().Be("admin");
        options.BrokerPassword.Should().Be("secret");
    }

    [Fact]
    public void RepositoryCredentials_CanBeSet()
    {
        // Arrange
        var options = new DistributionEventHubOptions();

        // Act
        options.RepositoryUser = "mongo-admin";
        options.RepositoryPassword = "mongo-secret";

        // Assert
        options.RepositoryUser.Should().Be("mongo-admin");
        options.RepositoryPassword.Should().Be("mongo-secret");
    }

    [Fact]
    public void TlsSettings_CanBeConfigured()
    {
        // Arrange
        var options = new DistributionEventHubOptions();

        // Act
        options.RepositoryUseTls = false;
        options.RepositoryAllowInsecureTls = true;

        // Assert
        options.RepositoryUseTls.Should().BeFalse();
        options.RepositoryAllowInsecureTls.Should().BeTrue();
    }

    [Fact]
    public void SystemDatabaseName_CanBeChanged()
    {
        // Arrange
        var options = new DistributionEventHubOptions();

        // Act
        options.SystemDatabaseName = "CustomSystemDb";

        // Assert
        options.SystemDatabaseName.Should().Be("CustomSystemDb");
    }

    [Fact]
    public void BrokerPort_CanBeChanged()
    {
        // Arrange
        var options = new DistributionEventHubOptions();

        // Act
        options.BrokerPort = 15672;

        // Assert
        options.BrokerPort.Should().Be(15672);
    }
}
