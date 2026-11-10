using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Configuration;

/// <summary>
///     AB#8623: the connections the library opens itself use the same credentials as MassTransit's bus connection.
/// </summary>
[Trait("Category", "Unit")]
public class BrokerConnectionFactoryTests
{
    [Fact]
    public void Create_NoBrokerUserConfigured_KeepsTheRabbitMqClientDefaults_LikeMassTransit()
    {
        // The local default: BrokerUser/BrokerPassword are not set, ConfigureRabbitMqTransportOptions maps the nulls.
        var options = TransportOptionsFrom(new DistributionEventHubOptions());
        options.User.Should().BeNull();
        options.Pass.Should().BeNull();

        var factory = BrokerConnectionFactory.Create(options, "bridge");

        factory.UserName.Should().Be(ConnectionFactory.DefaultUser).And.Be("guest");
        factory.Password.Should().Be(ConnectionFactory.DefaultPass).And.Be("guest");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Create_BlankBrokerUser_KeepsTheRabbitMqClientDefaults(string blank)
    {
        var factory = BrokerConnectionFactory.Create(new RabbitMqTransportOptions { User = blank, Pass = blank }, "bridge");

        factory.UserName.Should().Be(ConnectionFactory.DefaultUser);
        factory.Password.Should().Be(ConnectionFactory.DefaultPass);
    }

    [Fact]
    public void Create_BrokerUserConfigured_UsesIt()
    {
        var options = TransportOptionsFrom(new DistributionEventHubOptions
        {
            BrokerUser = "octo-adapter", BrokerPassword = "adapter-secret"
        });

        var factory = BrokerConnectionFactory.Create(options, "bridge");

        factory.UserName.Should().Be("octo-adapter");
        factory.Password.Should().Be("adapter-secret");
    }

    [Fact]
    public void Create_OnlyUserConfigured_DecidesUserAndPasswordIndependently_LikeMassTransit()
    {
        var factory = BrokerConnectionFactory.Create(new RabbitMqTransportOptions { User = "octo-mq-user", Pass = null },
            "bridge");

        factory.UserName.Should().Be("octo-mq-user");
        factory.Password.Should().Be(ConnectionFactory.DefaultPass);
    }

    [Fact]
    public void Create_AppliesHostPortVirtualHostTlsAndClientName()
    {
        var factory = BrokerConnectionFactory.Create(new RabbitMqTransportOptions
        {
            Host = "broker.example", Port = 5671, VHost = "octo", UseSsl = true
        }, "dev-hangfire-legacy-binding-cleanup");

        factory.HostName.Should().Be("broker.example");
        factory.Port.Should().Be(5671);
        factory.VirtualHost.Should().Be("octo");
        factory.ClientProvidedName.Should().Be("dev-hangfire-legacy-binding-cleanup");
        factory.Ssl.Enabled.Should().BeTrue();
        factory.Ssl.ServerName.Should().Be("broker.example");
    }

    [Fact]
    public void Create_EmptyVirtualHost_UsesTheRootVirtualHost()
    {
        var factory = BrokerConnectionFactory.Create(new RabbitMqTransportOptions { VHost = "" }, "bridge");

        factory.VirtualHost.Should().Be("/");
        factory.Ssl.Enabled.Should().BeFalse();
    }

    private static RabbitMqTransportOptions TransportOptionsFrom(DistributionEventHubOptions hubOptions)
    {
        var options = new RabbitMqTransportOptions();
        new ConfigureRabbitMqTransportOptions(Options.Create(hubOptions)).Configure(options);
        return options;
    }
}
