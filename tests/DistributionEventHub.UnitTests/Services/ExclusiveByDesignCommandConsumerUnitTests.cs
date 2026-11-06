using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

/// <summary>
///     Declaring a command consumer exclusive by design with a reason (AB#8363): validation of the declaration and
///     which registrations get the AB#6909 deprecation notice. The broker topology is pinned by the integration tests
///     <c>ExclusiveByDesignCommandTests</c>.
/// </summary>
[Trait("Category", "Unit")]
public class ExclusiveByDesignCommandConsumerUnitTests
{
    private const string Reason = "addressed to one pipeline instance";

    public static TheoryData<string> EmptyReasons => new() { "", " ", "\t\n" };

    [Fact]
    public void Factory_SetsTheReason_AndNothingElse()
    {
        var options = CommandConsumerOptions.ExclusiveByDesign(Reason);

        options.ExclusiveByDesignReason.Should().Be(Reason);
        options.Should().Be(new CommandConsumerOptions { ExclusiveByDesignReason = Reason });
    }

    [Fact]
    public void Default_DeclaresNothing()
    {
        new CommandConsumerOptions().ExclusiveByDesignReason.Should().BeNull("the declaration is opt-in");
    }

    [Theory]
    [MemberData(nameof(EmptyReasons))]
    public void Factory_RejectsAnEmptyReason(string reason)
    {
        var act = () => CommandConsumerOptions.ExclusiveByDesign(reason);

        act.Should().Throw<ArgumentException>();
    }

    [Theory]
    [MemberData(nameof(EmptyReasons))]
    public void AddCommandConsumer_RejectsAnEmptyReason(string reason)
    {
        var act = () => AddConsumer(new CommandConsumerOptions { ExclusiveByDesignReason = reason });

        act.Should().Throw<ArgumentException>().WithMessage("*ExclusiveByDesignReason*empty*");
    }

    [Theory]
    [MemberData(nameof(EmptyReasons))]
    public void RegisterCommandConsumer_RejectsAnEmptyReason_BeforeTouchingTheBus(string reason)
    {
        var (sut, bus, _) = NewEventHubControl();

        var act = () => sut.RegisterCommandConsumer<string>("cmd", (_, _) => Task.CompletedTask,
            new CommandConsumerOptions { ExclusiveByDesignReason = reason });

        act.Should().Throw<ArgumentException>().WithMessage("*ExclusiveByDesignReason*empty*");
        bus.Invocations.Should().BeEmpty();
    }

    public static TheoryData<CommandConsumerOptions, string> Contradictions => new()
    {
        { new CommandConsumerOptions { ExclusiveByDesignReason = Reason, MigratesLegacyCommand = true }, "*MigratesLegacyCommand*contradict*" },
        { new CommandConsumerOptions { ExclusiveByDesignReason = Reason, LegacyProbeInterval = TimeSpan.FromSeconds(1) }, "*LegacyProbeInterval*" },
        { new CommandConsumerOptions { ExclusiveByDesignReason = Reason, ConfigureConsumeTopology = true }, "*ConfigureConsumeTopology*" }
    };

    [Theory]
    [MemberData(nameof(Contradictions))]
    public void AddCommandConsumer_RejectsSharedTopologyOptions(CommandConsumerOptions options, string message)
    {
        var act = () => AddConsumer(options);

        act.Should().Throw<ArgumentException>().WithMessage(message);
    }

    [Theory]
    [MemberData(nameof(Contradictions))]
    public void RegisterCommandConsumer_RejectsSharedTopologyOptions(CommandConsumerOptions options, string message)
    {
        var (sut, bus, _) = NewEventHubControl();

        var act = () => sut.RegisterCommandConsumer<string>("cmd", (_, _) => Task.CompletedTask, options);

        act.Should().Throw<ArgumentException>().WithMessage(message);
        bus.Invocations.Should().BeEmpty();
    }

    [Fact]
    public void AddCommandConsumer_ExclusiveByDesign_RegistersNoDeprecationObserver_NoSharedDefinition()
    {
        var services = AddConsumer(CommandConsumerOptions.ExclusiveByDesign(Reason));

        services.Should().NotContain(d => d.ServiceType == typeof(IBusObserver),
            "neither the deprecation notice nor the legacy bridge applies to an exclusive-by-design consumer");
        services.Should().NotContain(d =>
            d.ServiceType == typeof(IConsumerDefinition<DistributedConsumer<NoopConsumer, string>>));
    }

    [Fact]
    public void AddCommandConsumer_WithoutDeclaration_StillRegistersTheDeprecationObserver()
    {
        var services = AddConsumer(null);

        services.Should().ContainSingle(d => d.ServiceType == typeof(IBusObserver));
    }

    [Fact]
    public void RegisterCommandConsumer_ExclusiveByDesign_UsesTheClassicQueueName_AndLogsNoWarning()
    {
        var (sut, bus, logger) = NewEventHubControl();
        var command = $"cmd-{Guid.NewGuid():N}";

        sut.RegisterCommandConsumer<string>(command, (_, _) => Task.CompletedTask,
            CommandConsumerOptions.ExclusiveByDesign(Reason));

        bus.Verify(b => b.ConnectReceiveEndpoint($"test-{command}", It.IsAny<Action<IReceiveEndpointConfigurator>>()),
            Times.Once, "the queue keeps the classic name: no ::shared queue");
        logger.Levels.Should().NotContain(LogLevel.Warning);
    }

    [Fact]
    public void RegisterCommandConsumer_WithoutOptions_StillLogsTheDeprecationWarning()
    {
        var (sut, _, logger) = NewEventHubControl();
        var command = $"cmd-{Guid.NewGuid():N}";

        sut.RegisterCommandConsumer<string>(command, (_, _) => Task.CompletedTask);

        logger.Levels.Should().ContainSingle().Which.Should().Be(LogLevel.Warning);
        logger.Messages.Single().Should().Contain($"test-{command}").And.Contain("Deprecated (AB#6909)");
    }

    private static ServiceCollection AddConsumer(CommandConsumerOptions? options)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o => o.InstancePrefix = "test");
        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "svc";
            if (options is null)
            {
                config.AddCommandConsumer<NoopConsumer, string>("cmd");
            }
            else
            {
                config.AddCommandConsumer<NoopConsumer, string>("cmd", options);
            }
        });
        return services;
    }

    private static (IEventHubControl Sut, Mock<IBusControl> Bus, CapturingLogger<EventHubControl> Logger)
        NewEventHubControl()
    {
        var bus = new Mock<IBusControl>();
        bus.Setup(b => b.ConnectReceiveEndpoint(It.IsAny<string>(), It.IsAny<Action<IReceiveEndpointConfigurator>>()))
            .Returns(Mock.Of<HostReceiveEndpointHandle>());
        var address = new Mock<IBroadcastServiceAddress>();
        address.SetupGet(a => a.InstancePrefix).Returns("test");
        var logger = new CapturingLogger<EventHubControl>();
        var sut = new EventHubControl(bus.Object, address.Object, logger,
            Options.Create(new RabbitMqTransportOptions()), NullLoggerFactory.Instance);
        return (sut, bus, logger);
    }

    private sealed class NoopConsumer : IDistributedConsumer<string>
    {
        public Task ConsumeAsync(IDistributedContext<string> context) => Task.CompletedTask;
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<string> Messages { get; } = [];
        public List<LogLevel> Levels { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Levels.Add(logLevel);
            Messages.Add(formatter(state, exception));
        }
    }
}
