using Meshmakers.Common.CommandLineParser;
using Meshmakers.Common.CommandLineParser.Commands;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SampleCli.Configuration.Options;

namespace SampleCli.Commands;

public class PostCacheFileCommand : Command<OctoMonitoringOptions>
{
    private readonly IDistributedCacheService _distributedCacheService;
    private readonly IArgument _fileArg;
    private readonly IArgument _tenantIdArg;

    public PostCacheFileCommand(ILogger<PostCacheFileCommand> logger,
        IOptions<OctoMonitoringOptions> options, IDistributedCacheService distributedCacheService)
        : base(logger, "postCacheFile", "Posts a file to the cache", options)
    {
        _distributedCacheService = distributedCacheService;

        _tenantIdArg = CommandArgumentValue.AddArgument("tid", "tenantId", new[] { "TenantId to use" }, true, 1);
        _fileArg = CommandArgumentValue.AddArgument("f", "file", new[] { "File to cache" }, true, 1);
    }

    public override async Task Execute()
    {
        var rtModelFilePath = CommandArgumentValue.GetArgumentScalarValue<string>(_fileArg);

        var tenantId = CommandArgumentValue.GetArgumentScalarValue<string>(_tenantIdArg);

        Logger.LogInformation("File cache command executing");

        var fileName = Path.GetFileName(rtModelFilePath);
        var id = await _distributedCacheService.CreateStreamAsync(tenantId, File.OpenRead(rtModelFilePath), "application/octet-stream",
            fileName);

        Logger.LogInformation("File cached with id {Id}", id);

        Logger.LogInformation("See you next time!");
    }
}