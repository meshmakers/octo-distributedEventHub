namespace SampleCli.Configuration.Options;

/// <summary>
/// Options of the octo monitoring
/// </summary>
public class OctoMonitoringOptions
{
    /// <summary>
    ///     Constructor
    /// </summary>
    public OctoMonitoringOptions()
    {
        MessageBrokerHost = "localhost";
        MessageBrokerUser = "guest";
        MessageBrokerPassword = "guest";
        RepositoryHost = "localhost:27017";
        RepositoryUser = "octo-system-admin";
        RepositoryPassword = "OctoAdmin1";
        RepositoryUseTls = false;
    }
    
    /// <summary>
    ///     Gets or sets the message broker host name
    /// </summary>
    public string MessageBrokerHost { get; set; }
    
    /// <summary>
    ///     Gets or sets the message broker user
    /// </summary>
    public string MessageBrokerUser { get; set; }

    /// <summary>
    ///     Gets or sets the message broker password
    /// </summary>
    public string MessageBrokerPassword { get; set; }

    /// <summary>
    /// Gets or sets the repository host name
    /// </summary>
    public string RepositoryHost { get; set; }
    
    /// <summary>
    /// Gets or sets the repository user
    /// </summary>
    public string RepositoryUser { get; set; }
    
    /// <summary>
    /// Gets or sets the repository password
    /// </summary>
    public string RepositoryPassword { get; set; }

    /// <summary>
    /// Gets or sets whether to use TLS for repository connection
    /// </summary>
    public bool RepositoryUseTls { get; set; }

    /// <summary>
    /// Gets or sets whether to allow insecure TLS for repository connection
    /// </summary>
    public bool RepositoryAllowInsecureTls { get; set; }
}