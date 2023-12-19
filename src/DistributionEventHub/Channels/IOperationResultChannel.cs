namespace Meshmakers.Octo.Common.DistributionEventHub.Channels;

/// <summary>
/// Interface of an operation
/// </summary>
/// <typeparam name="TResult">Type of operation results</typeparam>
public interface IOperationResultChannel<out TResult> : IDisposable
{
    /// <summary>
    ///     Unsubscribe the channel
    /// </summary>
    /// <returns>Task</returns>
    Task UnsubscribeAsync();
}