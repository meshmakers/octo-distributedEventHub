namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

/// <summary>
///     Represents a persistent repository client
/// </summary>
public interface IRepositoryClient
{
    /// <summary>
    ///     Returns true when the data source is connected
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    ///     Gets a repository for the given tenant id
    /// </summary>
    /// <param name="repositoryName">The name of repository.</param>
    /// <returns></returns>
    Task<IRepository> GetRepositoryAsync(string repositoryName);

    /// <summary>
    ///     Starts a session
    /// </summary>
    /// <returns></returns>
    Task<IRepositorySession> StartSessionAsync();
}