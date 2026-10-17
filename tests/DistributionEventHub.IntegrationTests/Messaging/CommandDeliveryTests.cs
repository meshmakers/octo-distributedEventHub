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
///     Delivery information (redelivery / retry), fault suppression and non-persistent sends of shared command
///     endpoints (CK v2 review findings H5, M5, L7).
/// </summary>
[Collection("RabbitMQ")]
[Trait("Category", "Integration")]
public class CommandDeliveryTests(RabbitMqFixture rabbitMq) : IAsyncLifetime
{
    private readonly List<ServiceProvider> _providers = [];
    private readonly string _instancePrefix = $"cmddel-{Guid.NewGuid():N}";

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

    private Task<IConnection> RawConnectionAsync() =>
        new ConnectionFactory
        {
            HostName = rabbitMq.Host, Port = rabbitMq.Port, UserName = rabbitMq.Username,
            Password = rabbitMq.Password
        }.CreateConnectionAsync(TestContext.Current.CancellationToken);

    private string Prefixed(string commandName) => $"{_instancePrefix}-{commandName}";

    private static string NewCommandName() => $"octo::method::test::type::m-{Guid.NewGuid():N}";

    [Fact]
    public async Task UnackedMessage_IsRedelivered_HandlerSeesRedeliveredFlag_AndTransientDeliveryMode()
    {
        var commandName = NewCommandName();
        var queue = Prefixed(commandName);
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("Client");

        // 1. a raw consumer takes the first delivery and never acks it (simulates a killed handler process)
        await using var connection = await RawConnectionAsync();
        var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await channel.ExchangeDeclareAsync(queue + "::send", "fanout", durable: true, autoDelete: false,
            cancellationToken: TestContext.Current.CancellationToken);
        await channel.QueueBindAsync(queue, queue + "::send", "",
            cancellationToken: TestContext.Current.CancellationToken);
        var firstDelivery = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var rawConsumer = new AsyncEventingBasicConsumer(channel);
        rawConsumer.ReceivedAsync += (_, ea) =>
        {
            firstDelivery.TrySetResult(ea.BasicProperties.Persistent);
            return Task.CompletedTask; // no ack
        };
        await channel.BasicConsumeAsync(queue, autoAck: false, rawConsumer,
            TestContext.Current.CancellationToken);

        var request = client.GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                new MethodTestRequest { Value = "x" },
                new CommandSendOptions { Mandatory = true, Persistent = false, Timeout = TimeSpan.FromSeconds(20) },
                TestContext.Current.CancellationToken);

        var persistent = await firstDelivery.Task.WaitAsync(TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken);
        persistent.Should().BeFalse("Persistent = false must send a transient message");

        // 2. the real handler endpoint attaches, then the raw consumer dies without ack → broker redelivery
        var deliveries = new ConcurrentBag<CommandDeliveryInfo>();
        await using var handle = handlerHost.GetRequiredService<IEventHubControl>()
            .RegisterCommandConsumer<MethodTestRequest>(commandName, async (message, delivery, respond) =>
            {
                deliveries.Add(delivery);
                await respond(new MethodTestResponse { Value = message.Value, Instance = "h" });
            }, new CommandConsumerOptions { SuppressFaults = true });
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(TestContext.Current.CancellationToken);

        var response = await request;

        response.Value.Should().Be("x");
        var delivery = deliveries.Should().ContainSingle().Subject;
        delivery.Redelivered.Should().BeTrue();
        delivery.IsPossibleDuplicate.Should().BeTrue();
    }

    [Fact]
    public async Task FirstDelivery_IsNotAPossibleDuplicate()
    {
        var commandName = NewCommandName();
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("Client");
        var deliveries = new ConcurrentBag<CommandDeliveryInfo>();

        await using var handle = handlerHost.GetRequiredService<IEventHubControl>()
            .RegisterCommandConsumer<MethodTestRequest>(commandName, async (message, delivery, respond) =>
            {
                deliveries.Add(delivery);
                await respond(new MethodTestResponse { Value = message.Value, Instance = "h" });
            }, new CommandConsumerOptions { SuppressFaults = true });
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        await client.GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                new MethodTestRequest { Value = "x" },
                new CommandSendOptions { Mandatory = true, Persistent = false, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken);

        var delivery = deliveries.Should().ContainSingle().Subject;
        delivery.Redelivered.Should().BeFalse();
        delivery.RetryAttempt.Should().Be(0);
        delivery.IsPossibleDuplicate.Should().BeFalse();
    }

    [Fact]
    public async Task InProcessRetry_IsVisibleAsRetryAttempt()
    {
        var commandName = NewCommandName();
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("Client");
        var deliveries = new ConcurrentBag<CommandDeliveryInfo>();

        await using var handle = handlerHost.GetRequiredService<IEventHubControl>()
            .RegisterCommandConsumer<MethodTestRequest>(commandName, async (message, delivery, respond) =>
            {
                deliveries.Add(delivery);
                if (delivery.RetryAttempt == 0)
                {
                    throw new InvalidOperationException("first attempt fails");
                }

                await respond(new MethodTestResponse { Value = message.Value, Instance = "h" });
            }, new CommandConsumerOptions());
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        await client.GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                new MethodTestRequest { Value = "x" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(10) },
                TestContext.Current.CancellationToken);

        deliveries.Select(d => d.RetryAttempt).Order().Should().Equal(0, 1);
        deliveries.Single(d => d.RetryAttempt == 1).IsPossibleDuplicate.Should().BeTrue();
    }

    [Fact]
    public async Task SuppressFaults_ThrowingHandler_RunsOnce_NoErrorQueue_RequesterTimesOut()
    {
        var commandName = NewCommandName();
        var handlerHost = await StartBusAsync("HandlerHost");
        var client = await StartBusAsync("Client");
        var executions = 0;

        await using var handle = handlerHost.GetRequiredService<IEventHubControl>()
            .RegisterCommandConsumer<MethodTestRequest>(commandName, (_, _, _) =>
            {
                Interlocked.Increment(ref executions);
                throw new InvalidOperationException("boom");
            }, new CommandConsumerOptions { SuppressFaults = true });
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        var act = () => client.GetRequiredService<IDistributionEventHubService>()
            .GetCommandResponseAsync<MethodTestRequest, MethodTestResponse>(commandName,
                new MethodTestRequest { Value = "secret" },
                new CommandSendOptions { Mandatory = true, Timeout = TimeSpan.FromSeconds(3) },
                TestContext.Current.CancellationToken);

        // no Fault<T> reaches the requester (it would surface as DistributedOperationFailedException)
        await act.Should().ThrowAsync<DistributionTimeoutException>();
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        executions.Should().Be(1);

        await using var connection = await RawConnectionAsync();
        await using var channel =
            await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var errorQueue = () => channel.QueueDeclarePassiveAsync(Prefixed(commandName) + "_error",
            TestContext.Current.CancellationToken);
        await errorQueue.Should().ThrowAsync<OperationInterruptedException>("no _error queue may hold the payload");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UndeserializableMessage_ErrorAndSkippedQueues_OnlyWithoutSuppressFaults(bool suppressFaults)
    {
        var commandName = NewCommandName();
        var queue = Prefixed(commandName);
        var handlerHost = await StartBusAsync("HandlerHost");
        await using var handle = handlerHost.GetRequiredService<IEventHubControl>()
            .RegisterCommandConsumer<MethodTestRequest>(commandName,
                (_, _, _) => Task.CompletedTask, new CommandConsumerOptions { SuppressFaults = suppressFaults });
        await handle.WaitUntilReadyAsync(TestContext.Current.CancellationToken);

        await using var connection = await RawConnectionAsync();
        await using (var publishChannel =
                     await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken))
        {
            // a body MassTransit cannot deserialize (stands for a message with a sensitive payload that faults
            // before reaching the handler, e.g. after a contract change)
            await publishChannel.BasicPublishAsync(queue + "::send", "", false,
                new BasicProperties { ContentType = "application/vnd.masstransit+json", Persistent = false },
                "{\"newPassword\":\"S3cret\""u8.ToArray(), TestContext.Current.CancellationToken);
        }

        async Task<bool> ExistsAsync(string name)
        {
            await using var channel =
                await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
            try
            {
                await channel.QueueDeclarePassiveAsync(name, TestContext.Current.CancellationToken);
                return true;
            }
            catch (OperationInterruptedException)
            {
                return false;
            }
        }

        var deadline = DateTime.UtcNow.AddSeconds(5);
        var anyCopy = false;
        while (DateTime.UtcNow < deadline && !anyCopy)
        {
            anyCopy = await ExistsAsync(queue + "_error") || await ExistsAsync(queue + "_skipped");
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }

        anyCopy.Should().Be(!suppressFaults,
            suppressFaults
                ? "SuppressFaults discards faulted/skipped messages instead of keeping a copy"
                : "the default moves the message to the _error/_skipped queue");
    }
}
