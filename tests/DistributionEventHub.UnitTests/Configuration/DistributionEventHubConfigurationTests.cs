using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Configuration;

[Trait("Category", "Unit")]
public class DistributionEventHubConfigurationTests
{
    [Fact]
    public void AddDistributionEventHub_NoUniqueServiceAddress_ThrowsException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "test";
        });

        // Act & Assert
        var action = () => services.AddDistributionEventHub(config =>
        {
            // UniqueServiceAddress not set
        });

        action.Should().Throw<DistributedOperationFailedException>()
            .WithMessage("*UniqueServiceAddress*");
    }

    [Fact]
    public void AddDistributionEventHub_ValidConfiguration_RegistersEventHubService()
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
    }

    [Fact]
    public void AddDistributionEventHub_ValidConfiguration_RegistersBroadcastServiceAddress()
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
            config.InstancePrefix = "from-code"; // Override
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var serviceAddress = provider.GetService<IBroadcastServiceAddress>();
        serviceAddress!.InstancePrefix.Should().Be("from-code");
    }

    [Fact]
    public void AddDistributionEventHub_EmptyUniqueServiceAddress_ThrowsException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "test";
        });

        // Act & Assert
        var action = () => services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = ""; // Empty
        });

        action.Should().Throw<DistributedOperationFailedException>()
            .WithMessage("*UniqueServiceAddress*");
    }

    [Fact]
    public void AddDistributionEventHubWithOptions_ConfiguresBothOptionsAndEventHub()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddDistributionEventHubWithOptions(
            options =>
            {
                options.InstancePrefix = "test-instance";
                options.BrokerHost = "rabbitmq.local";
                options.BrokerPort = 5672;
            },
            config =>
            {
                config.UniqueServiceAddress = "TestService";
            });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetService<IOptions<DistributionEventHubOptions>>();
        options.Should().NotBeNull();
        options!.Value.InstancePrefix.Should().Be("test-instance");
        options.Value.BrokerHost.Should().Be("rabbitmq.local");
    }

    [Fact]
    public void AddDistributionEventHub_WhitespaceUniqueServiceAddress_ThrowsException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = "test";
        });

        // Act & Assert
        var action = () => services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "   "; // Whitespace only
        });

        action.Should().Throw<DistributedOperationFailedException>()
            .WithMessage("*UniqueServiceAddress*");
    }

    [Fact]
    public void AddDistributionEventHub_ServiceNameIsSetCorrectly()
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
            config.UniqueServiceAddress = "MyTestService";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var serviceAddress = provider.GetService<IBroadcastServiceAddress>();
        serviceAddress!.ServiceName.Should().Be("MyTestService");
    }

    [Fact]
    public void AddDistributionEventHub_ReturnsBuilder()
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
        var builder = services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "TestService";
        });

        // Assert
        builder.Should().NotBeNull();
        builder.Services.Should().BeSameAs(services);
    }
}
