namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration.Options;

/// <summary>
///     Options of distributed cache with pub sub
/// </summary>
public class DistributionEventHubOptions
{
    /// <summary>
    ///     Constructor
    /// </summary>
    public DistributionEventHubOptions()
    {
        BrokerHost = "localhost";
        RepositoryHost = "localhost";
        SystemDatabaseName = "OctoSystem";
        RepositoryUser = "octo-system-admin";
        DatabaseAuthenticationSource = "admin";
    }

    /// <summary>
    ///     The name of the message broker host
    /// </summary>
    public string BrokerHost { get; set; }


    /// <summary>
    ///     The password to connect to message broker
    /// </summary>
    public string? BrokerUser { get; set; }

    /// <summary>
    ///     The password to connect to message broker
    /// </summary>
    public string? BrokerPassword { get; set; }

    /// <summary>
    ///     Database host name for storage of sagas and temporary files
    /// </summary>
    public string RepositoryHost { get; set; }

    /// <summary>
    ///     System database name for storage of sagas and temporary files
    /// </summary>
    public string SystemDatabaseName { get; set; }

    /// <summary>
    ///     User name to access database for storage of sagas and temporary files
    /// </summary>
    public string RepositoryUser { get; set; }

    /// <summary>
    ///     User password to access database for storage of sagas and temporary files
    /// </summary>
    public string? RepositoryPassword { get; set; } = null!;

    /// <summary>
    ///     Set to true to use TLS for database connection
    /// </summary>
    public bool RepositoryUseTls { get; set; } = true;

    /// <summary>
    ///     Set to true to allow insecure TLS for database connection
    /// </summary>
    public bool RepositoryAllowInsecureTls { get; set; } = false;

    /// <summary>
    ///     The authentication source for the database
    /// </summary>
    public string DatabaseAuthenticationSource { get; set; }
}