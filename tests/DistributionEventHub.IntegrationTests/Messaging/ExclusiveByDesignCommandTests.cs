using System.Collections.Concurrent;
using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     A command consumer declared exclusive by design with a reason (AB#8363) keeps the classic exclusive topology
///     (exclusive queue under the classic name, no <c>::shared</c> queue, no <c>::send</c> exchange, classic senders
///     unchanged) and logs no AB#6909 deprecation warning; a registration without a declaration still warns.
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class ExclusiveByDesignCommandTests(RabbitMqFixture rabbitMq) : IAsyncLifetime
{
    private const string CommandName = "exclusive::cmd";
    private const string Reason = "addressed to one pipeline instance";

    private readonly List<ServiceProvider> _providers = [];
    private readonly CapturingLoggerProvider _logs = new();
    private readonly MigrationLedger _ledger = new();
    private readonly string _instancePrefix = $"exclusive-{Guid.NewGuid():N}";

    private string Prefixed => $"{_instancePrefix}-{CommandName}";

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

    private async Task<ServiceProvider> StartAsync(string name, Action<IDistributionEventHubConfiguration> register)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(_logs).SetMinimumLevel(LogLevel.Debug));
        services.AddSingleton(_ledger);
        services.AddSingleton(new ReplicaIdentity(name));
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
            config.UniqueServiceAddress = name;
            config.AutomaticallyStartBusDuringStartup = false;
            register(config);
        });

        var provider = services.BuildServiceProvider();
        _providers.Add(provider);
        await provider.GetRequiredService<IEventHubControl>().StartAsync(TestContext.Current.CancellationToken);
        return provider;
    }

    private Task<ServiceProvider> StartClassicSenderAsync() => StartAsync("Sender",
        config => config.AddCommandClient<MigrationRequest>(CommandName, TimeSpan.FromSeconds(10)));

    private static Task<MigrationResponse> SendClassicAsync(ServiceProvider sender, string id) =>
        sender.GetRequiredService<ICommandClient<MigrationRequest>>().GetResponse<MigrationResponse>(
            new MigrationRequest { Id = id }, TestContext.Current.CancellationToken, TimeSpan.FromSeconds(10));

    private IEnumerable<string> DeprecationWarnings => _logs.Entries
        .Where(e => e.Level == LogLevel.Warning && e.Message.Contains("Deprecated (AB#6909)")
                                                && e.Message.Contains(Prefixed))
        .Select(e => e.Message);

    private async Task AssertClassicExclusiveTopologyAsync()
    {
        await using var connection = await new ConnectionFactory
        {
            HostName = rabbitMq.Host, Port = rabbitMq.Port, UserName = rabbitMq.Username,
            Password = rabbitMq.Password
        }.CreateConnectionAsync(TestContext.Current.CancellationToken);

        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(Prefixed)))
            .Should().Be(405, "the queue keeps the classic name and is exclusive to the owner's connection");
        (await ReplyCodeOfAsync(connection, c => c.QueueDeclarePassiveAsync(Prefixed + "::shared")))
            .Should().Be(404, "an exclusive-by-design consumer declares no shared queue");
        (await ReplyCodeOfAsync(connection, c => c.ExchangeDeclarePassiveAsync(Prefixed + "::send")))
            .Should().Be(404, "an exclusive-by-design consumer declares no send exchange");
    }

    private static async Task<int?> ReplyCodeOfAsync(IConnection connection, Func<IChannel, Task> action)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            await action(channel);
            return null;
        }
        catch (OperationInterruptedException e)
        {
            return e.ShutdownReason?.ReplyCode;
        }
    }

    [Fact]
    public async Task AddCommandConsumer_ExclusiveByDesign_KeepsClassicTopology_AndLogsNoDeprecationWarning()
    {
        await StartAsync("Owner", config => config.AddCommandConsumer<MigrationConsumer, MigrationRequest>(
            CommandName, CommandConsumerOptions.ExclusiveByDesign(Reason)));
        var sender = await StartClassicSenderAsync();

        (await SendClassicAsync(sender, "x")).Replica.Should().Be("Owner", "classic senders are unchanged");
        await AssertClassicExclusiveTopologyAsync();
        DeprecationWarnings.Should().BeEmpty();
    }

    [Fact]
    public async Task AddCommandConsumer_WithoutDeclaration_StillLogsTheDeprecationWarning()
    {
        await StartAsync("Owner", config => config.AddCommandConsumer<MigrationConsumer, MigrationRequest>(
            CommandName));

        DeprecationWarnings.Should().ContainSingle();
    }

    [Fact]
    public async Task RegisterCommandConsumer_ExclusiveByDesign_KeepsClassicTopology_AndLogsNoDeprecationWarning()
    {
        var owner = await StartAsync("Owner", _ => { });
        var sender = await StartClassicSenderAsync();

        await using var handle = owner.GetRequiredService<IEventHubControl>().RegisterCommandConsumer<MigrationRequest>(
            CommandName,
            async (message, respond) =>
                await respond(new MigrationResponse { Id = message.Id, Replica = "Owner" }),
            CommandConsumerOptions.ExclusiveByDesign(Reason));
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        (await SendClassicAsync(sender, "x")).Replica.Should().Be("Owner", "classic senders are unchanged");
        await AssertClassicExclusiveTopologyAsync();
        DeprecationWarnings.Should().BeEmpty();
    }

    [Fact]
    public async Task RegisterCommandConsumer_WithoutDeclaration_StillLogsTheDeprecationWarning()
    {
        var owner = await StartAsync("Owner", _ => { });

        await using var handle = owner.GetRequiredService<IEventHubControl>().RegisterCommandConsumer<MigrationRequest>(
            CommandName,
            async (message, respond) =>
                await respond(new MigrationResponse { Id = message.Id, Replica = "Owner" }));
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        DeprecationWarnings.Should().ContainSingle();
    }

    private sealed record LogEntry(LogLevel Level, string Message);

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<LogEntry> _entries = new();

        public IReadOnlyCollection<LogEntry> Entries => _entries.ToArray();

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(_entries);

        public void Dispose()
        {
        }

        private sealed class CapturingLogger(ConcurrentQueue<LogEntry> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter) =>
                entries.Enqueue(new LogEntry(logLevel, formatter(state, exception)));
        }
    }
}
