using Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository.MongoDb;

/// <summary>
///     Implementation of <see cref="IRepositoryClient" /> that uses a mongodb database for storage
/// </summary>
public class RepositoryClient : IRepositoryClient
{
    private readonly MongoClient _client;
    private readonly DistributionEventHubOptions _options;

    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="options"></param>
    public RepositoryClient(ILogger<RepositoryClient> logger, IOptions<DistributionEventHubOptions> options)
    {
        _options = options.Value;

        var urlBuilder = new MongoUrlBuilder();

        if (_options.RepositoryHost.Contains(","))
        {
            urlBuilder.Servers =
                _options.RepositoryHost.Split(",").Select(x => new MongoServerAddress(x));
        }
        else
        {
            urlBuilder.Server = new MongoServerAddress(_options.RepositoryHost);
        }

        if (!string.IsNullOrWhiteSpace(_options.RepositoryUser)
            && !string.IsNullOrWhiteSpace(_options.RepositoryPassword))
        {
            urlBuilder.Username = _options.RepositoryUser;
            urlBuilder.Password = _options.RepositoryPassword;
            urlBuilder.DatabaseName = _options.SystemDatabaseName;
            urlBuilder.AuthenticationSource = _options.DatabaseAuthenticationSource;
        }

        urlBuilder.UseTls = _options.RepositoryUseTls;
        urlBuilder.AllowInsecureTls = _options.RepositoryAllowInsecureTls;

        var settings = MongoClientSettings.FromUrl(urlBuilder.ToMongoUrl());
        settings.ReadConcern = ReadConcern.Majority;
        settings.WriteConcern = new WriteConcern(WriteConcern.WMode.Majority, TimeSpan.FromSeconds(2));
        settings.ClusterConfigurator = cb =>
        {
            cb.Subscribe<CommandStartedEvent>(e =>
            {
                // logger.LogDebug("{ObjCommandName} - {Json}", e.CommandName, e.Command.ToJson());
            });
        };
        _client = new MongoClient(settings);
    }

    /// <inheritdoc />
    public Task<IRepository> GetRepositoryAsync(string repositoryName)
    {
        IRepository repository =
            new Repository(_client.GetDatabase(repositoryName.ToLower()));
        return Task.FromResult(repository);
    }

    /// <inheritdoc />
    public bool IsConnected => true;

    /// <inheritdoc />
    public Task<IRepositorySession> StartSessionAsync()
    {
        throw new NotImplementedException();
    }
}