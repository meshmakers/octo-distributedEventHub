namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Delegate for executing commands
/// </summary>
/// <typeparam name="TMessage"></typeparam>
public delegate Task ExecuteCommandHandler<in TMessage>(TMessage message, RespondToCommandHandler respondToCommand);

/// <summary>
/// Delegate for handling responses to commands
/// </summary>
public delegate Task RespondToCommandHandler(object response);

/// <summary>
/// Delegate for executing commands with transport delivery information (redelivery / retry detection).
/// </summary>
/// <typeparam name="TMessage">Type of the command message</typeparam>
public delegate Task ExecuteDeliveredCommandHandler<in TMessage>(TMessage message, CommandDeliveryInfo delivery,
    RespondToCommandHandler respondToCommand);

/// <summary>
/// Transport delivery information of a command message.
/// </summary>
/// <param name="Redelivered">
/// <c>true</c> when the broker delivered the message before without an acknowledgement (consumer connection lost,
/// process killed, broker restart). The previous delivery may or may not have been processed.
/// </param>
/// <param name="RetryAttempt">In-process retry attempt of the bus retry filter (0 = first attempt).</param>
/// <param name="RedeliveryCount">Scheduled-redelivery count (0 unless redelivery is configured).</param>
/// <param name="MessageId">Transport message id, if any.</param>
public sealed record CommandDeliveryInfo(bool Redelivered, int RetryAttempt, int RedeliveryCount, Guid? MessageId)
{
    /// <summary>
    /// <c>true</c> when the message may have been processed before: broker redelivery, in-process retry or
    /// scheduled redelivery. Non-idempotent handlers must not execute it again.
    /// </summary>
    public bool IsPossibleDuplicate => Redelivered || RetryAttempt > 0 || RedeliveryCount > 0;
}
