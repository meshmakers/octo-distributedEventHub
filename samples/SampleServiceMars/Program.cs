using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using SampleEvents.Consumers;
using SampleEvents.Messages;
using SampleServiceMars.Services;
using SampleServiceMars.StateMachine;

namespace SampleServiceMars;

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
                services.AddHostedService<TestService>();
                services.AddDistributionEventHub(configuration =>
                {
                    configuration.UniqueServiceAddress = "SampleServiceMars";
                    
                    // Example: Configure instance prefix for multi-instance deployments
                    // This ensures complete isolation between different OctoMesh instances
                    // configuration.InstancePrefix = "prod"; // or "dev", "staging", "test-env-1", etc.
                    configuration.InstancePrefix = "Mars"; // Example instance prefix
                    
                    //configuration.AddCommandConsumer<SampleCommandRequestConsumer, SampleCommandRequest>("SampleCommandRequest");
                    configuration.AddRoutedEventConsumer<CreateTenantConsumer, CreateTenant>();
                    configuration.AddRoutedEventConsumer<UpdateEventConsumer, UpdateEvent>();
                    configuration.AddBroadcastEventConsumer<ReloadTenantConsumer, ReloadTenant>();
                    configuration.AddBroadcastEventConsumer<BroadcastTestConsumer, BroadcastTest>();
                    configuration.AddCommandClient<ReserveStockRequest>("reserve-stock");
                    // configuration.AddSagaStateMachine<OrderStateMachine, OrderState>()
                    //     .MongoDbRepository(r =>
                    //     {
                    //         r.Connection =
                    //             "mongodb://octo-system-admin:<password>@localhost:27017/?authSource=admin&readPreference=primary&directConnection=true&ssl=false";
                    //         r.DatabaseName = "orderdb";
                    //     });
                });
            });
    }
}