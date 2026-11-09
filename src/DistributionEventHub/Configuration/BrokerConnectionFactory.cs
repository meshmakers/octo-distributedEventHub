using MassTransit;
using RabbitMQ.Client;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration;

/// <summary>
///     Builds the RabbitMQ <see cref="ConnectionFactory" /> for the connections the library opens itself, next to
///     MassTransit's bus connection (legacy command bridge, Hangfire scheduler topology cleanup).
/// </summary>
/// <remarks>
///     Credentials follow MassTransit 8.5.10 exactly (AB#8623): <c>RabbitMqRegistrationBusFactory</c> passes
///     <see cref="RabbitMqTransportOptions.User" /> / <see cref="RabbitMqTransportOptions.Pass" /> to the host only when
///     they are not null or whitespace, and <c>RabbitMqAddressExtensions.GetConnectionFactory</c> again sets
///     <see cref="ConnectionFactory.UserName" /> / <see cref="ConnectionFactory.Password" /> only when they are not null or
///     whitespace. Otherwise the RabbitMQ client's own defaults stay (<see cref="ConnectionFactory.DefaultUser" /> and
///     <see cref="ConnectionFactory.DefaultPass" />, both <c>guest</c>). User and password are decided independently.
///     A factory that assigned the null values directly failed with <c>Value cannot be null (Parameter 'userName')</c>
///     whenever no broker user was configured (the local default), while the bus itself connected.
/// </remarks>
internal static class BrokerConnectionFactory
{
    /// <summary>
    ///     Creates a factory for host, port, virtual host, TLS and credentials of <paramref name="options" />.
    /// </summary>
    public static ConnectionFactory Create(RabbitMqTransportOptions options, string clientProvidedName)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.Host,
            Port = options.Port,
            VirtualHost = string.IsNullOrEmpty(options.VHost) ? "/" : options.VHost,
            ClientProvidedName = clientProvidedName
        };
        ApplyCredentials(factory, options);
        if (options.UseSsl)
        {
            factory.Ssl = new SslOption { Enabled = true, ServerName = options.Host };
        }

        return factory;
    }

    /// <summary>
    ///     Applies the configured user and password the way MassTransit does: a value that is null or whitespace is not
    ///     applied, so the RabbitMQ client default stays.
    /// </summary>
    public static void ApplyCredentials(ConnectionFactory factory, RabbitMqTransportOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.User))
        {
            factory.UserName = options.User;
        }

        if (!string.IsNullOrWhiteSpace(options.Pass))
        {
            factory.Password = options.Pass;
        }
    }
}
