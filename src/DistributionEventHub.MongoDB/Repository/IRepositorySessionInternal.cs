using Meshmakers.Octo.Common.DistributionEventHub.Repository;
using MongoDB.Driver;

namespace Meshmakers.Octo.Common.DistributionEventHub.MongoDB.Repository;

/// <summary>
///     Internal version of <see cref="IRepositorySession" />
/// </summary>
internal interface IRepositorySessionInternal : IRepositorySession
{
    IClientSessionHandle SessionHandle { get; }
}