using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

/// <summary>
///     The options overload has a default implementation so test doubles that implement
///     <see cref="IEventHubControl" /> directly keep compiling (AB#5709).
/// </summary>
public class EventHubControlDefaultImplementationTests
{
    [Fact]
    public async Task OptionsOverload_DelegatesToPlainOverload()
    {
        IEventHubControl sut = new MinimalEventHubControl();
        string? received = null;
        RoutedEventDeliveryContext? context = null;

        sut.RegisterRoutedEventConsumer<string>("queue-a",
            (message, delivery) =>
            {
                received = message;
                context = delivery;
                return Task.CompletedTask;
            }, RoutedEventConsumerOptions.LatestOnly);

        var minimal = (MinimalEventHubControl)sut;
        minimal.Address.Should().Be("queue-a");
        await minimal.Handler!("hello");
        received.Should().Be("hello");
        context.Should().Be(RoutedEventDeliveryContext.None);
    }

    [Fact]
    public void OptionsOverload_ValidatesOptions()
    {
        IEventHubControl sut = new MinimalEventHubControl();

        var act = () => sut.RegisterRoutedEventConsumer<string>("queue-a", (_, _) => Task.CompletedTask,
            new RoutedEventConsumerOptions { CoalescePendingMessages = true, PrefetchCount = 4 });

        act.Should().Throw<ArgumentException>();
    }

    private sealed class MinimalEventHubControl : IEventHubControl
    {
        public string? Address { get; private set; }
        public Func<string, Task>? Handler { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public EndpointHandle RegisterRoutedEventConsumer<TMessage>(string destinationAddress,
            Func<TMessage, Task> handler) where TMessage : class
        {
            Address = destinationAddress;
            Handler = message => handler((TMessage)(object)message);
            return new EndpointHandle(Mock.Of<HostReceiveEndpointHandle>());
        }

        public EndpointHandle RegisterRoutedEventConsumer<TMessage>(Func<TMessage, Task> handler)
            where TMessage : class => throw new NotSupportedException();

        public EndpointHandle RegisterRoutedEventConsumer<TMessage>(string exchangeName, string routingKey,
            Func<TMessage, Task> handler) where TMessage : class => throw new NotSupportedException();

        public EndpointHandle RegisterCommandConsumer<TMessage>(string commandName,
            ExecuteCommandHandler<TMessage> handler) where TMessage : class => throw new NotSupportedException();
    }
}
