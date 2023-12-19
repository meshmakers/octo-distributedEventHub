using System.Reflection;
using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using SampleEvents.Consumers;
using SampleEvents.Messages;

namespace SampleServiceMoon
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            await CreateHostBuilder(args).Build().RunAsync();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.Configure<DistributionEventHubOptions>(options => hostContext.Configuration.GetSection("DistributionEventHub").Bind(options));
                    services.AddDistributionEventHub((configuration) =>
                    {
                        configuration.UniqueServiceAddress = "SampleServiceMoon";
                        
                        configuration.AddBroadcastEventConsumer<ReloadTenantConsumer, ReloadTenant>("SampleServiceMoon");
                        configuration.AddCommandConsumer<ReserveStockRequestConsumer, ReserveStockRequest>("reserve-stock");
                    });
                });
    }
}
