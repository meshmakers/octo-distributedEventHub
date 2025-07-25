using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using SampleEvents.Consumers;
using SampleEvents.Messages;

namespace SampleServiceEarth;

public class Program
{
    public static async Task Main(string[] args)
    {
        await CreateHostBuilder(args).Build().RunAsync();
    }

    public static IHostBuilder CreateHostBuilder(string[] args)
    {
        return Host.CreateDefaultBuilder(args)
            .ConfigureServices((hostContext, services) =>
            {
                services.Configure<DistributionEventHubOptions>(options =>
                    hostContext.Configuration.GetSection("DistributionEventHub").Bind(options));
                services.AddDistributionEventHub(configuration =>
                {
                    configuration.UniqueServiceAddress = "SampleServiceEarth";

                    // InstancePrefix can be set in two ways:
                    // 1. Via appsettings.json (recommended) - automatically applied from DistributionEventHubOptions
                    // 2. Via code (takes precedence) - uncomment line below:
                    // configuration.InstancePrefix = "prod"; // or "dev", "staging", etc.

                    configuration.AddRoutedEventConsumer<CreateTenantConsumer, CreateTenant>();
                    configuration.AddRoutedEventConsumer<UpdateEventConsumer, UpdateEvent>();
                    configuration.AddBroadcastEventConsumer<ReloadTenantConsumer, ReloadTenant>();
                    configuration.AddBroadcastEventConsumer<BroadcastTestConsumer, BroadcastTest>();
                    configuration.AddRoutedEventConsumer<OrderAcceptedConsumer, OrderAccepted>();
                });
            });
    }
}