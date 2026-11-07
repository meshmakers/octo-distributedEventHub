using Testcontainers.RabbitMq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;

public class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container;

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(5672);
    /// <summary>
    ///     The user the tests connect as. <c>guest</c> (full permissions, like the core services' <c>octo-mq-user</c>) unless
    ///     the environment variable <c>OCTO_DEH_IT_AS_ADAPTER</c> is <c>1</c>: then the whole suite runs as the restricted
    ///     adapter user (AB#7338) — every test that uses no <c>octo::method::</c> name must still pass.
    /// </summary>
    public string Username => RunAsAdapter ? AdapterUsername : "guest";

    public string Password => RunAsAdapter ? AdapterPassword : "guest";

    /// <summary>A user with full permissions (the core services' broker user), whatever <see cref="Username" /> is.</summary>
    public string CoreUsername => "guest";

    public string CorePassword => "guest";

    /// <summary>The restricted adapter user (permissions of <see cref="RestrictedAdapterUser" />).</summary>
    public string AdapterUsername => RestrictedAdapterUser.Name;

    public string AdapterPassword => "adapter-it-throwaway";

    private static bool RunAsAdapter => Environment.GetEnvironmentVariable("OCTO_DEH_IT_AS_ADAPTER") == "1";

    public RabbitMqFixture()
    {
        _container = new RabbitMqBuilder("rabbitmq:3-management")
            .WithUsername("guest")
            .WithPassword("guest")
            .Build();
    }

    /// <summary>The broker's own log (stdout and stderr of the container), e.g. to count channel exceptions.</summary>
    public async Task<string> GetBrokerLogAsync()
    {
        var (stdout, stderr) = await _container.GetLogsAsync();
        return stdout + stderr;
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        await RestrictedAdapterUser.CreateAsync(_container, AdapterPassword);
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("RabbitMQ")]
public class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>
{
}
