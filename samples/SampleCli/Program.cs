// See https://aka.ms/new-console-template for more information

using Meshmakers.Common.CommandLineParser;
using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Common.Configuration;
using Meshmakers.Common.Shared.Services;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NLog;
using NLog.Extensions.Logging;
using SampleCli;
using SampleCli.Commands;
using SampleCli.Configuration;
using SampleCli.Configuration.Options;
using SampleEvents.Consumers;
using SampleEvents.Messages;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

static IServiceProvider BuildDi()
{
    var services = new ServiceCollection();

    
    services.AddSingleton<IConsoleService, ConsoleService>();
    services.AddSingleton<IEnvironmentService, EnvironmentService>();
    services.AddSingleton<IParserService, ParserService>();
    services.AddSingleton<ICommandParser, CommandParser>();
    services.AddSingleton<IConfigWriter, ConfigWriter>(provider =>
    {
        var configWriter = new ConfigWriter();
        configWriter.AddOptions(Constants.OctoMonitoringToolOptionsRootNode,
            provider.GetRequiredService<IOptions<OctoMonitoringOptions>>());
        return configWriter;
    });
    services.AddDistributionEventHub(c =>
    {
        c.UniqueServiceAddress = "OctoDistributionEventHubMonitorCli";
        
        c.AddCommandClient<SampleCommandRequest>("SampleCommandRequest");
    });

    services.AddTransient<Runner>();

    services.AddTransient<ICommand, ConfigOctoCommand>();
    services.AddTransient<ICommand, PublishDirectEventsCommand>();
    services.AddTransient<ICommand, PublishTenantCreateCommand>();
    services.AddTransient<ICommand, PublishTenantReloadCommand>();
    services.AddTransient<ICommand, PostCacheFileCommand>();
    services.AddTransient<ICommand, GetCacheFileCommand>();
    services.AddTransient<ICommand, SendCommand>();
    services.AddTransient<ICommand, StartSagaCommand>();

    services.ConfigureOptions<ConfigureDistributionEventHubOptions>();

    var config = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", true, true)
        .AddJsonFile(
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                $".{Constants.OctoDistributionEventMonitorUserFolderName}{Path.DirectorySeparatorChar}settings.json"),
            true, true)
        .Build();

    services.AddLogging(loggingBuilder =>
    {
        loggingBuilder.ClearProviders();
        loggingBuilder.SetMinimumLevel(LogLevel.Trace);
        loggingBuilder.AddNLog(config);
    });
    
    var serviceProvider = services.BuildServiceProvider();
    return serviceProvider;
}

var logger = LogManager.GetCurrentClassLogger();
IEventHubControl? eventHubControl = null;
try
{
    var serviceProvider = BuildDi();
    using (serviceProvider as IDisposable)
    {
        eventHubControl = serviceProvider.GetRequiredService<IEventHubControl>();
        await eventHubControl.StartAsync();
        
        var runner = serviceProvider.GetRequiredService<Runner>();
        return await runner.DoActionAsync();
    }
}
catch (Exception ex)
{
    logger.Error(ex, "Stopped program because of exception");
    return -100;
}
finally
{
    if (eventHubControl != null)
    {
        await eventHubControl.StopAsync();
    }

    // Ensure to flush and stop internal timers/threads before application-exit (Avoid segmentation fault on Linux)
    LogManager.Shutdown();
}