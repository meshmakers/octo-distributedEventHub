using MongoDB.Driver;

namespace Meshmakers.Octo.Common.DistributionEventHub.Repository.MongoDb;

internal class RepositorySession : IRepositorySessionInternal
{
    internal RepositorySession(IClientSessionHandle clientSessionHandle, string applicationName)
    {
        SessionHandle = clientSessionHandle;
        ApplicationName = applicationName;
    }

    public string ApplicationName { get; set; }

    public void Dispose()
    {
        SessionHandle.Dispose();
    }

    public void StartTransaction()
    {
        SessionHandle.StartTransaction();
    }

    public async Task CommitTransactionAsync()
    {
        await SessionHandle.CommitTransactionAsync().ConfigureAwait(false);
    }

    public async Task AbortTransactionAsync()
    {
        await SessionHandle.AbortTransactionAsync().ConfigureAwait(false);
    }

    public IClientSessionHandle SessionHandle { get; }
}