using System.Collections.Concurrent;
using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Messaging;

/// <summary>
///     AB#7338 (B0a): the restricted broker user of adapters and edge sites (<see cref="RestrictedAdapterUser" />) works for
///     everything an adapter does on the bus and is refused on every CK method resource
///     (<c>octo::method::</c>): consume, bind, declare, delete and publish. A "core" bus with the full-permission user
///     keeps serving the method queue.
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class RestrictedAdapterUserTests(RabbitMqFixture rabbitMq) : IAsyncLifetime
{
    private const int AccessRefused = 403;

    private readonly List<ServiceProvider> _providers = [];
    private readonly string _instancePrefix = $"b0a-{Guid.NewGuid():N}";
    private readonly string _methodCommand = $"octo::method::test::type::m-{Guid.NewGuid():N}-1";

    private string MethodQueue => $"{_instancePrefix}-{_methodCommand}";
    private string MethodSendExchange => MethodQueue + "::send";

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

    private async Task<ServiceProvider> StartBusAsync(string serviceName, string user, string password)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = _instancePrefix;
            o.BrokerHost = rabbitMq.Host;
            o.BrokerPort = (ushort)rabbitMq.Port;
            o.BrokerUser = user;
            o.BrokerPassword = password;
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

    private Task<ServiceProvider> StartAdapterBusAsync(string name) =>
        StartBusAsync(name, rabbitMq.AdapterUsername, rabbitMq.AdapterPassword);

    private Task<ServiceProvider> StartCoreBusAsync(string name) =>
        StartBusAsync(name, rabbitMq.CoreUsername, rabbitMq.CorePassword);

    private async Task<IConnection> ConnectAsync(string user, string password) =>
        await new ConnectionFactory
        {
            HostName = rabbitMq.Host, Port = rabbitMq.Port, UserName = user, Password = password
        }.CreateConnectionAsync(TestContext.Current.CancellationToken);

    /// <summary>Runs the action on a fresh channel; the AMQP reply code of the refusal, or <c>null</c> when it was accepted.</summary>
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

    private async Task<ServiceProvider> StartMethodHandlerAsync(ConcurrentBag<string> received)
    {
        var core = await StartCoreBusAsync("CoreHandler");
        var handle = core.GetRequiredService<IEventHubControl>().RegisterCommandConsumer<MethodTestRequest>(
            _methodCommand,
            async (message, respond) =>
            {
                received.Add(message.Value);
                await respond(new MethodTestResponse { Value = message.Value, Instance = "core" });
            },
            new CommandConsumerOptions { SuppressFaults = true });
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
        return core;
    }

    [Fact]
    public async Task AdapterUser_RunsTheCommandBusOfAnAdapter()
    {
        var host = await StartAdapterBusAsync("AdapterHost");
        var client = await StartAdapterBusAsync("AdapterClient");
        var control = host.GetRequiredService<IEventHubControl>();
        var hub = client.GetRequiredService<IDistributionEventHubService>();

        // shared (competing consumer) command with send exchange, mandatory send, TTL, response
        await using var shared = control.RegisterCommandConsumer<MethodTestRequest>("adapter::shared",
            async (m, respond) => await respond(new MethodTestResponse { Value = m.Value, Instance = "shared" }),
            new CommandConsumerOptions());
        await shared.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
        var sharedResponse = await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>("adapter::shared",
            new MethodTestRequest { Value = "a" },
            new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
            TestContext.Current.CancellationToken);
        sharedResponse.Instance.Should().Be("shared");

        // shared command with fault suppression and prefetch: the options a CK method handler uses, on a non-method name
        await using var suppressed = control.RegisterCommandConsumer<MethodTestRequest>("adapter::suppressed",
            async (m, respond) => await respond(new MethodTestResponse { Value = m.Value, Instance = "suppressed" }),
            new CommandConsumerOptions { SuppressFaults = true, PrefetchCount = 4, ConcurrentMessageLimit = 4 });
        await suppressed.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
        (await hub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>("adapter::suppressed",
                new MethodTestRequest { Value = "b" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken)).Instance.Should().Be("suppressed");

        // classic exclusive command (the pipeline-addressed commands of the adapter)
        await using var exclusive = control.RegisterCommandConsumer<MethodTestRequest>("adapter::exclusive",
            async (m, respond) => await respond(new MethodTestResponse { Value = m.Value, Instance = "exclusive" }),
            CommandConsumerOptions.ExclusiveByDesign("addressed to one pipeline instance"));
        await exclusive.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        // routed event over a topic exchange (explicit queue with a binding)
        var routed = new ConcurrentBag<string>();
        await using var topic = control.RegisterRoutedEventConsumer<MethodTestRequest>("adapter-topic", "adapter.#",
            m =>
            {
                routed.Add(m.Value);
                return Task.CompletedTask;
            });
        await Task.Delay(500, TestContext.Current.CancellationToken);
        await hub.SendToExchangeAsync("adapter-topic", "adapter.event", new MethodTestRequest { Value = "evt" },
            TestContext.Current.CancellationToken);
        await Fixtures.TestHelpers.WaitForCondition(() => routed.Contains("evt"), TimeSpan.FromSeconds(10));
        routed.Should().Contain("evt");
    }

    [Fact]
    public async Task AdapterUser_IsRefusedOnEveryMethodResource_AndTheCoreHandlerKeepsServing()
    {
        var received = new ConcurrentBag<string>();
        var core = await StartMethodHandlerAsync(received);
        var coreHub = core.GetRequiredService<IDistributionEventHubService>();

        // sanity: the method queue works for the core user
        (await coreHub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(_methodCommand,
                new MethodTestRequest { Value = "core-call" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken)).Instance.Should().Be("core");

        await using var adapter = await ConnectAsync(rabbitMq.AdapterUsername, rabbitMq.AdapterPassword);

        // consume: the handler's share of the calls
        (await ReplyCodeOfAsync(adapter, c => c.BasicConsumeAsync(MethodQueue, autoAck: true,
                new AsyncEventingBasicConsumer(c))))
            .Should().Be(AccessRefused, "no read permission on the method queue");

        // read one message directly
        (await ReplyCodeOfAsync(adapter, c => c.BasicGetAsync(MethodQueue, autoAck: true)))
            .Should().Be(AccessRefused);

        // bind a queue of its own to the send exchange (a second consumer path)
        (await ReplyCodeOfAsync(adapter, async c =>
            {
                var q = await c.QueueDeclareAsync(string.Empty, durable: false, exclusive: true, autoDelete: true);
                await c.QueueBindAsync(q.QueueName, MethodSendExchange, string.Empty);
            }))
            .Should().Be(AccessRefused, "no read permission on the send exchange");

        // declare the queue, the send exchange; delete the queue
        (await ReplyCodeOfAsync(adapter, c => c.QueueDeclareAsync(MethodQueue, durable: true, exclusive: false,
                autoDelete: true)))
            .Should().Be(AccessRefused, "no configure permission on the method queue");
        (await ReplyCodeOfAsync(adapter, c => c.ExchangeDeclareAsync(MethodSendExchange, ExchangeType.Fanout,
                durable: true, autoDelete: false)))
            .Should().Be(AccessRefused, "no configure permission on the send exchange");
        (await ReplyCodeOfAsync(adapter, c => c.QueueDeleteAsync(MethodQueue)))
            .Should().Be(AccessRefused);
        (await ReplyCodeOfAsync(adapter, c => c.QueuePurgeAsync(MethodQueue)))
            .Should().Be(AccessRefused);

        // publish a forged call to the send exchange: the broker closes the channel, the handler never sees it
        var publishChannel = await adapter.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var closed = new TaskCompletionSource<ShutdownEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        publishChannel.ChannelShutdownAsync += (_, e) =>
        {
            closed.TrySetResult(e);
            return Task.CompletedTask;
        };
        await publishChannel.BasicPublishAsync(MethodSendExchange, string.Empty, body: "{}"u8.ToArray(),
            cancellationToken: TestContext.Current.CancellationToken);
        var shutdown = await closed.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        shutdown.ReplyCode.Should().Be(AccessRefused, "no write permission on the send exchange");

        await Task.Delay(500, TestContext.Current.CancellationToken);
        received.Should().BeEquivalentTo(["core-call"], "the forged call never reached the handler");

        // the core handler still serves
        (await coreHub.GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(_methodCommand,
                new MethodTestRequest { Value = "core-call-2" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken)).Instance.Should().Be("core");
    }

    [Fact]
    public async Task AdapterUser_CannotInjectACallIntoAMethodQueueViaTheDefaultExchange()
    {
        var received = new ConcurrentBag<string>();
        await StartMethodHandlerAsync(received);
        await using var adapter = await ConnectAsync(rabbitMq.AdapterUsername, rabbitMq.AdapterPassword);

        // A publish to the default exchange with the queue name as routing key reaches ANY queue and is checked against the
        // write permission of "amq.default" only, not against the queue: without the exclusion the call would be injected.
        var channel = await adapter.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var closed = new TaskCompletionSource<ShutdownEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.ChannelShutdownAsync += (_, e) =>
        {
            closed.TrySetResult(e);
            return Task.CompletedTask;
        };
        await channel.BasicPublishAsync(string.Empty, MethodQueue, mandatory: true,
            basicProperties: new BasicProperties(), body: "{}"u8.ToArray(),
            cancellationToken: TestContext.Current.CancellationToken);

        var shutdown = await closed.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        shutdown.ReplyCode.Should().Be(AccessRefused, "no write permission on the default exchange (amq.default)");
    }

    [Fact]
    public async Task AdapterUser_CannotRegisterAMethodHandler()
    {
        var adapter = await StartAdapterBusAsync("RogueAdapter");

        var act = async () =>
        {
            await using var handle = adapter.GetRequiredService<IEventHubControl>()
                .RegisterCommandConsumer<MethodTestRequest>(_methodCommand,
                    async (m, respond) => await respond(new MethodTestResponse { Value = m.Value, Instance = "rogue" }),
                    new CommandConsumerOptions { SuppressFaults = true });
            await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
        };
        await act.Should().ThrowAsync<Exception>("the broker refuses the queue, so the endpoint never becomes ready");

        // nothing was declared: a mandatory send finds no queue
        var core = await StartCoreBusAsync("CoreSender");
        var send = () => core.GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(_methodCommand,
                new MethodTestRequest { Value = "x" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken);
        await send.Should().ThrowAsync<CommandNotRoutableException>();
    }
}
