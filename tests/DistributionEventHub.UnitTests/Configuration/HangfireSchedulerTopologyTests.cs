using FluentAssertions;
using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Configuration;

/// <summary>
///     AB#5867: the Hangfire scheduler endpoint must be instance-scoped.
/// </summary>
[Trait("Category", "Unit")]
public class HangfireSchedulerTopologyTests
{
    [Fact]
    public void AddHangfireMessageScheduler_UsesAnInstancePrefixedSchedulerQueue()
    {
        var provider = BuildProvider("dev", setPrefixInCode: null);

        provider.GetRequiredService<IOptions<HangfireEndpointOptions>>().Value.QueueName
            .Should().Be("dev-hangfire");
    }

    [Fact]
    public void AddHangfireMessageScheduler_PrefixSetInCodeAfterTheCall_IsStillApplied()
    {
        // config.InstancePrefix "takes precedence" and may be assigned after AddHangfireMessageScheduler;
        // the queue name must follow the final value, not the one at call time.
        var provider = BuildProvider("fromOptions", setPrefixInCode: "Main");

        provider.GetRequiredService<IOptions<HangfireEndpointOptions>>().Value.QueueName
            .Should().Be("main-hangfire");
    }

    [Fact]
    public void AddHangfireMessageScheduler_RegistersTheLegacyBindingRemover()
    {
        // Resolving every IBusObserver would need a Hangfire JobStorage (MassTransit's own observer);
        // the descriptor is enough to prove the remover is wired, and resolving it proves its factory.
        var services = BuildServices("dev", setPrefixInCode: null);
        var descriptor = services.Should().ContainSingle(d =>
            d.ServiceType == typeof(IBusObserver) && d.ImplementationFactory != null).Subject;

        using var provider = services.BuildServiceProvider();
        descriptor.ImplementationFactory!(provider).Should().BeOfType<LegacyHangfireSchedulerBindingRemover>();
    }

    [Fact]
    public void GetQueueName_NeverReturnsTheSharedLegacyName()
    {
        HangfireSchedulerTopology.GetQueueName("main").Should().Be("main-hangfire");
        HangfireSchedulerTopology.GetQueueName("dev").Should().Be("dev-hangfire");
        HangfireSchedulerTopology.GetQueueName("dev").Should().NotBe(HangfireSchedulerTopology.LegacyQueueName);
    }

    private static ServiceProvider BuildProvider(string optionsPrefix, string? setPrefixInCode) =>
        BuildServices(optionsPrefix, setPrefixInCode).BuildServiceProvider();

    private static ServiceCollection BuildServices(string optionsPrefix, string? setPrefixInCode)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<DistributionEventHubOptions>(o =>
        {
            o.InstancePrefix = optionsPrefix;
            o.BrokerHost = "localhost";
        });

        services.AddDistributionEventHub(config =>
        {
            config.UniqueServiceAddress = "BotService";
            config.AddHangfireMessageScheduler();
            if (setPrefixInCode is not null)
            {
                config.InstancePrefix = setPrefixInCode;
            }
        });

        return services;
    }
}
