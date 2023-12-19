using Meshmakers.Octo.Common.DistributionEventHub.Payloads;
using Meshmakers.Octo.Common.Shared;
using StackExchange.Redis;

namespace Meshmakers.Octo.Common.DistributionEventHub.Channels;

/// <summary>
/// Implements an operation result channel
/// </summary>
/// <typeparam name="TResult">Result type of operation results</typeparam>
public class OperationResultChannel<TResult> : IOperationResultChannel<TResult>
{
    private readonly string _currentClientName;
    private readonly ChannelMessageQueue _channelMessageQueue;
    private readonly TaskCompletionSource<TResult> _taskCompletionSource;
    private readonly CancellationTokenSource _cancellationTokenSource;
    
    /// <summary>
    /// Returns the task of the operation result
    /// </summary>
    public Task<TResult> Task => _taskCompletionSource.Task;

    internal OperationResultChannel(string currentClientName,
        ChannelMessageQueue channelMessageQueue)
    {
        _currentClientName = currentClientName;
        _channelMessageQueue = channelMessageQueue;
        _taskCompletionSource = new TaskCompletionSource<TResult>();

        _cancellationTokenSource = new CancellationTokenSource(5000);
    }
    
    public async Task<TResult?> GetResultAsync()
    {
    //_cancellationTokenSource.Token.Register(() => _taskCompletionSource.SetCanceled());
        var channelMessage = await _channelMessageQueue.ReadAsync(_cancellationTokenSource.Token);
        if (!channelMessage.Message.HasValue)
        {
            return default;
        }
        
        string? serializedObject = channelMessage.Message;
        if (string.IsNullOrWhiteSpace(serializedObject))
        {
            return default;
        }
        
        var o = serializedObject.Deserialize<ChannelOperationResult<TResult>>();
        if (o.SenderClientName == _currentClientName)
        {
            return default;
        }
        
        if (o.Result != null)
        {
            _taskCompletionSource.SetResult(o.Result);
        }
        else if (o.Error != null)
        {
            _taskCompletionSource.SetException(new DistributedOperationFailedException<OperationError>(o.Error));
        }
        else
        {
            _taskCompletionSource.SetException(new Exception("Operation failed"));
        }


            
        // _channelMessageQueue.OnMessage(channelMessage =>
        // {
        //     if (!channelMessage.Message.HasValue)
        //     {
        //         return;
        //     }
        //
        //     string? serializedObject = channelMessage.Message;
        //     if (string.IsNullOrWhiteSpace(serializedObject))
        //     {
        //         return;
        //     }
        //
        //     var o = serializedObject.Deserialize<ChannelOperationResult<TResult>>();
        //     if (o.SenderClientName == currentClientName)
        //     {
        //         return;
        //     }
        //
        //     if (o.Result != null)
        //     {
        //         _taskCompletionSource.SetResult(o.Result);
        //     }
        //     else if (o.Error != null)
        //     {
        //         _taskCompletionSource.SetException(new DistributedOperationFailedException<OperationError>(o.Error));
        //     }
        //     else
        //     {
        //         _taskCompletionSource.SetException(new Exception("Operation failed"));
        //     }
        // });
    }

    /// <inheritdoc />
    public Task UnsubscribeAsync()
    {
        return _channelMessageQueue.UnsubscribeAsync();
    }
    
    /// <inheritdoc />
    public void Dispose()
    {
        _channelMessageQueue.Unsubscribe();
    }
}