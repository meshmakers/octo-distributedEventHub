using Meshmakers.Common.CommandLineParser;
using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Octo.Common.DistributionEventHub;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;

namespace SampleCli.Commands;

public class GetCacheFileCommand : Command<OctoMonitoringOptions>
{
    private readonly IDistributedCacheService _distributedCacheService;
    private readonly IArgument _pathArg;
    private readonly IArgument _cacheKeyArg;
    private readonly IArgument _tenantIdArg;

    public GetCacheFileCommand(ILogger<GetCacheFileCommand> logger,
        IOptions<OctoMonitoringOptions> options, IDistributedCacheService distributedCacheService)
        : base(logger, "getCacheFile", "Gets a cache file based on its key", options)
    {
        _distributedCacheService = distributedCacheService;

        _tenantIdArg = CommandArgumentValue.AddArgument("tid", "tenantId", new[] { "TenantId to use" }, false, 1);
        _cacheKeyArg = CommandArgumentValue.AddArgument("k", "key", new[] { "Cache Key to file" }, true, 1);
        _pathArg = CommandArgumentValue.AddArgument("p", "path", new[] { "Directory path to save file" }, true, 1);
    }

    public override async Task Execute()
    {
        var path = CommandArgumentValue.GetArgumentScalarValue<string>(_pathArg);
        var id = CommandArgumentValue.GetArgumentScalarValue<string>(_cacheKeyArg);

        var tenantId = CommandArgumentValue.GetArgumentScalarValue<string>(_tenantIdArg);

        Logger.LogInformation("Get file cache command executing");

        var cacheStream = await _distributedCacheService.GetCacheStreamAsync(tenantId, id);
        if (cacheStream == null)
        {
            Logger.LogError("No file cached with id {Id}", id);
            return;
        }

        var filePath = Path.Combine(path, cacheStream.FileName);
        await using var fileStream = File.OpenWrite(filePath);
        byte[] buffer = new byte[8192]; // Buffer size can be adjusted
        int bytesRead;

        // Read from the input stream in chunks and write to the file stream
        while ((bytesRead = await cacheStream.Stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            fileStream.Write(buffer, 0, bytesRead);
        }
        
        Logger.LogInformation("File stored at '{FilePath}'", filePath);
    
        Logger.LogInformation("See you next time!");
    }
}