using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     Pins the topology and the send behaviour of the legacy (opt-out) command path, so the shared command
///     endpoints (AB#6891) stay strictly opt-in: <c>RegisterCommandConsumer(name, handler)</c> and the plain
///     <c>GetCommandResponseAsync</c> keep the exclusive, non-durable, auto-delete queue and the temporary request
///     exchange, and a send without consumer still ends in the timeout, not in a typed routing failure.
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class LegacyCommandEndpointUnchangedTests(RabbitMqFixture rabbitMq) : IAsyncLifetime
{
    private readonly List<ServiceProvider> _providers = [];
    private readonly string _instancePrefix = $"legacycmd-{Guid.NewGuid():N}";

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var provider in _providers)
        {
            try
            {
                await provider.GetRequiredService<IEventHubControl>().StopAsync();
            }
            catch
            {
                // ignore
            }

            await provider.DisposeAsync();
        }
    }

    private async Task<ServiceProvider> StartBusAsync(string serviceName)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = _instancePrefix;
            o.BrokerHost = rabbitMq.Host;
            o.BrokerPort = (ushort)rabbitMq.Port;
            o.BrokerUser = rabbitMq.Username;
            o.BrokerPassword = rabbitMq.Password;
        });
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = serviceName;
            config.AutomaticallyStartBusDuringStartup = false;
        });

        var provider = services.BuildServiceProvider();
        _providers.Add(provider);
        await provider.GetRequiredService<IEventHubControl>().StartAsync(TestContext.Current.CancellationToken);
        return provider;
    }

    private async Task<IConnection> RawConnectionAsync() =>
        await new ConnectionFactory
        {
            HostName = rabbitMq.Host, Port = rabbitMq.Port, UserName = rabbitMq.Username,
            Password = rabbitMq.Password
        }.CreateConnectionAsync(TestContext.Current.CancellationToken);

    [Fact]
    public async Task LegacyRegistration_KeepsExclusiveQueue_AndHasNoSharedSendExchange()
    {
        var commandName = $"legacy::cmd::{Guid.NewGuid():N}";
        var queue = $"{_instancePrefix}-{commandName}";
        var host = await StartBusAsync("LegacyHost");
        var client = await StartBusAsync("LegacyClient");

        await using var handle = host.GetRequiredService<IEventHubControl>().RegisterCommandConsumer<MethodTestRequest>(
            commandName,
            async (message, respond) =>
                await respond(new MethodTestResponse { Value = message.Value, Instance = "legacy" }));
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        var response = await client.GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                new MethodTestRequest { Value = "ping" }, TestContext.Current.CancellationToken,
                TimeSpan.FromSeconds(10));
        response.Value.Should().Be("ping");

        await using var connection = await RawConnectionAsync();

        // An exclusive queue cannot be accessed from another connection: passive declare is refused with 405.
        var queueChannel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var lockedQueue = async () => await queueChannel.QueueDeclarePassiveAsync(queue,
            TestContext.Current.CancellationToken);
        (await lockedQueue.Should().ThrowAsync<OperationInterruptedException>())
            .Which.ShutdownReason!.ReplyCode.Should().Be(405);

        // The shared-endpoint send exchange is not created by the legacy path.
        var exchangeChannel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var missingExchange = async () => await exchangeChannel.ExchangeDeclarePassiveAsync(queue + "::send",
            TestContext.Current.CancellationToken);
        (await missingExchange.Should().ThrowAsync<OperationInterruptedException>())
            .Which.ShutdownReason!.ReplyCode.Should().Be(404);
    }

    [Fact]
    public async Task LegacySend_WithoutConsumer_EndsInTimeout_NotInTypedRoutingFailure()
    {
        var client = await StartBusAsync("LegacyClient");

        var act = () => client.GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>($"legacy::cmd::{Guid.NewGuid():N}",
                new MethodTestRequest { Value = "x" }, TestContext.Current.CancellationToken,
                TimeSpan.FromSeconds(2));

        await act.Should().ThrowAsync<DistributionTimeoutException>();
    }
}
