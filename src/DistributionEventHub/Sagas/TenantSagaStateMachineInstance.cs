using MassTransit;

namespace Meshmakers.Octo.Common.DistributionEventHub.Sagas;

/// <summary>
/// Represents a tenant saga state machine instance
/// </summary>
public interface TenantSagaStateMachineInstance : SagaStateMachineInstance
{
    /// <summary>
    /// Gets or sets the current tenant id of the saga instance
    /// </summary>
    public string? TenantId { get; set; }
}