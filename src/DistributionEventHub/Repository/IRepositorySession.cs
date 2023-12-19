namespace Meshmakers.Octo.Common.DistributionEventHub.Repository;

/// <summary>
/// Represents a persistent repository session
/// </summary>
public interface IRepositorySession : IDisposable
{
    /// <summary>Starts a transaction.</summary>
    void StartTransaction();
    
    /// <summary>Commits the transaction.</summary>
    /// <returns></returns>
    Task CommitTransactionAsync();

    /// <summary>Aborts the transaction.</summary>
    /// <returns></returns>
    Task AbortTransactionAsync();
}