using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Meshmakers.Octo.Common.DistributionEventHub.Consumers;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

/// <summary>
///     Opt-in switch, naming rule and deprecation notice of the exclusive → shared command queue migration (AB#6909).
/// </summary>
[Trait("Category", "Unit")]
public class LegacyCommandMigrationUnitTests
{
    [Fact]
    public void Options_DefaultsKeepTheExistingBehaviour()
    {
        var options = new CommandConsumerOptions();

        options.MigratesLegacyCommand.Should().BeFalse("the migration is opt-in per consumer");
        options.LegacyProbeInterval.Should().BeNull();
        options.SuppressFaults.Should().BeFalse();
        options.ConfigureConsumeTopology.Should().BeFalse();
    }

    [Fact]
    public void MigratedQueueName_DiffersFromTheClassicName_AndKeepsThePrefix()
    {
        var classic = CacheCommon.ApplyInstancePrefix("Prod", "octo::bot::import-ck");

        CacheCommon.GetMigratedSharedCommandQueueName(classic).Should().Be("prod-octo::bot::import-ck::shared");
        CacheCommon.GetMigratedSharedCommandQueueName(classic).Should().NotBe(classic);
        CacheCommon.GetSharedCommandSendExchangeName(classic).Should().Be("prod-octo::bot::import-ck::send");
    }

    [Fact]
    public void ConfigurationInterface_SharedAddCommandConsumer_DefaultsToNotSupported()
    {
        // an implementation written before AB#6909 keeps compiling; calling the new member is a clear error
        IDistributionEventHubConfiguration sut = new Mock<IDistributionEventHubConfiguration>
            { CallBase = true }.Object;

        var act = () => sut.AddCommandConsumer<NoopConsumer, string>("cmd", new CommandConsumerOptions());

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void AddCommandConsumer_WithSuppressFaults_IsRejected_BecauseTheConsumerWrapperCannotHonourIt()
    {
        var services = NewServices();

        var act = () => services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "svc";
            config.AddCommandConsumer<NoopConsumer, string>("cmd", new CommandConsumerOptions { SuppressFaults = true });
        });

        act.Should().Throw<ArgumentException>().WithMessage("*SuppressFaults*");
    }

    [Fact]
    public void AddCommandConsumer_WithOptions_RegistersAnEndpointDefinitionForTheSharedQueue()
    {
        var services = NewServices();

        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "svc";
            config.AddCommandConsumer<NoopConsumer, string>("cmd",
                new CommandConsumerOptions { MigratesLegacyCommand = true });
        });

        services.Should().Contain(d =>
            d.ServiceType == typeof(IConsumerDefinition<DistributedConsumer<NoopConsumer, string>>));
    }

    [Fact]
    public void AddCommandConsumer_WithoutOptions_RegistersNoSharedEndpointDefinition()
    {
        var services = NewServices();

        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "svc";
            config.AddCommandConsumer<NoopConsumer, string>("cmd");
        });

        services.Should().NotContain(d =>
            d.ServiceType == typeof(IConsumerDefinition<DistributedConsumer<NoopConsumer, string>>));
    }

    [Fact]
    public void DeprecationNotice_IsLoggedOncePerCommandName_AndNamesTheMigrationSwitch()
    {
        LegacyCommandDeprecation.ResetForTests();
        var logger = new CapturingLogger();

        LegacyCommandDeprecation.WarnOnce(logger, "p-a");
        LegacyCommandDeprecation.WarnOnce(logger, "p-a");
        LegacyCommandDeprecation.WarnOnce(logger, ["p-a", "p-b", "p-c"]);
        LegacyCommandDeprecation.WarnOnce(logger, ["p-b", "p-c"]);

        logger.Messages.Should().HaveCount(2);
        logger.Messages[0].Should().Contain("p-a").And.Contain("MigratesLegacyCommand");
        logger.Messages[1].Should().Contain("p-b").And.Contain("p-c").And.NotContain("p-a");
        logger.Levels.Should().OnlyContain(l => l == LogLevel.Warning);
    }

    private static ServiceCollection NewServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o => o.InstancePrefix = "test");
        return services;
    }

    private sealed class NoopConsumer : IDistributedConsumer<string>
    {
        public Task ConsumeAsync(IDistributedContext<string> context) => Task.CompletedTask;
    }

    private sealed class CapturingLogger : ILogger
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
