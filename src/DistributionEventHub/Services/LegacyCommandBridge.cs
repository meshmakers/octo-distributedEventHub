using MassTransit;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
///     State of a <see cref="LegacyCommandBridge" /> after a reconcile pass.
/// </summary>
internal enum LegacyCommandBridgeState
{
    /// <summary>Not reconciled yet.</summary>
    Unknown,

    /// <summary>
    ///     A classic (exclusive) consumer of the command name is alive. The shared queue is NOT bound to the legacy
    ///     exchange, so that classic senders reach exactly that consumer and nothing is processed twice.
    /// </summary>
    LegacyConsumerPresent,

    /// <summary>No classic consumer is alive; the shared queue is bound to the legacy exchange and serves classic senders.</summary>
    Bound,

    /// <summary>The shared queue does not exist yet (its endpoint is still starting); retried on the next pass.</summary>
    SharedQueueMissing,

    /// <summary>The broker refused the reconcile (for example the legacy exchange was declared with other arguments).</summary>
    Faulted
}

/// <summary>
///     Lets a shared command queue serve senders that still use the classic topology, without a flag day and
///     without double processing (AB#6909).
/// </summary>
/// <remarks>
///     <para>
///         <b>Classic topology</b> of a command name <c>N</c> (with instance prefix): the consumer declares the
///         exclusive, non-durable, auto-delete queue <c>N</c> and the exchange <c>N</c> (fanout, non-durable,
///         auto-delete) bound to it; senders (<c>AddCommandClient</c>, <c>GetCommandResponseAsync</c>) publish to
///         <c>exchange:N?temporary=true</c>.
///     </para>
///     <para>
///         <b>Shared topology of a migrated command</b>: the queue <c>N::shared</c> (durable, auto-delete, NOT
///         exclusive) bound to <c>N::send</c>. The name differs on purpose: RabbitMQ refuses a redeclare of
///         <c>N</c> with other arguments (PRECONDITION_FAILED) and an exclusive queue cannot be touched at all by a
///         second connection (RESOURCE_LOCKED), so old and new replicas could not coexist on one name.
///     </para>
///     <para>
///         <b>The bridge</b> binds <c>N::shared</c> to the classic exchange <c>N</c> — but only while no classic
///         consumer exists. A fanout exchange copies a message into every bound queue, so binding while the classic
///         queue is alive would process every classic command twice. The bridge therefore probes the classic queue
///         (a passive declare answers 405 RESOURCE_LOCKED for an exclusive queue of another connection, 404 if it
///         does not exist) and reconciles on every pass: classic consumer present → unbind (new replicas stand by,
///         the old replica keeps working as before); absent → bind (new replicas take over). All replicas of the
///         shared consumer run the same idempotent reconcile, so they converge on one state.
///     </para>
///     <para>
///         Known windows, both bounded by the pass interval: after the last classic replica stopped, classic
///         senders reach no queue until the next pass; if a classic replica (re)starts while the shared queue is
///         bound (rollback), messages are copied to both until the next pass. Roll back by stopping the shared
///         replicas first.
///     </para>
/// </remarks>
internal sealed class LegacyCommandBridge : IAsyncDisposable
{
    internal static readonly TimeSpan DefaultInterval = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(15);

    private readonly RabbitMqTransportOptions _transportOptions;
    private readonly string _legacyName;
    private readonly string _sharedQueue;
    private readonly string _clientName;
    private readonly TimeSpan _interval;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _stopping = new();
    private IConnection? _connection;
    private Task? _loop;
    private int _disposed;

    /// <param name="transportOptions">Broker connection settings</param>
    /// <param name="legacyName">Prefixed command name = classic queue and exchange name</param>
    /// <param name="sharedQueue">Prefixed name of the shared queue</param>
    /// <param name="interval">Time between two reconcile passes</param>
    /// <param name="logger">Logger</param>
    public LegacyCommandBridge(RabbitMqTransportOptions transportOptions, string legacyName, string sharedQueue,
        TimeSpan? interval, ILogger logger)
    {
        _transportOptions = transportOptions;
        _legacyName = legacyName;
        _sharedQueue = sharedQueue;
        _clientName = $"{sharedQueue}-legacy-bridge";
        _interval = interval is { } i && i > TimeSpan.Zero ? i : DefaultInterval;
        _logger = logger;
    }

    /// <summary>Result of the last reconcile pass.</summary>
    public LegacyCommandBridgeState State { get; private set; }

    /// <summary>Starts the background reconcile loop (idempotent).</summary>
    public void Start()
    {
        if (_loop != null)
        {
            return;
        }

        _loop = Task.Run(() => RunAsync(_stopping.Token));
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_interval);
        do
        {
            await ReconcileAsync(cancellationToken).ConfigureAwait(false);
        } while (await WaitForNextTickAsync(timer, cancellationToken).ConfigureAwait(false));
    }

    private static async Task<bool> WaitForNextTickAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    ///     One reconcile pass. Never throws (broker problems are logged and reflected in <see cref="State" />).
    /// </summary>
    public async Task<LegacyCommandBridgeState> ReconcileAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var newState = await ReconcileCoreAsync(cancellationToken).ConfigureAwait(false);
            if (newState != State)
            {
                LogTransition(State, newState);
                State = newState;
            }

            return State;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return State;
        }
        catch (Exception e)
        {
            if (State != LegacyCommandBridgeState.Faulted)
            {
                _logger.LogWarning(e,
                    "Legacy command bridge '{SharedQueue}': reconcile failed; the shared queue keeps serving its own " +
                    "send exchange, classic senders may not reach it. Retrying every {Interval}",
                    _sharedQueue, _interval);
            }

            await ResetConnectionAsync().ConfigureAwait(false);
            State = LegacyCommandBridgeState.Faulted;
            return State;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<LegacyCommandBridgeState> ReconcileCoreAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);

        if (await ClassicQueueExistsAsync(connection, cancellationToken).ConfigureAwait(false))
        {
            await UnbindAsync(connection, cancellationToken).ConfigureAwait(false);
            return LegacyCommandBridgeState.LegacyConsumerPresent;
        }

        return await BindAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     405 (exclusive queue of another connection) and a successful declare both mean "a classic consumer
    ///     exists"; 404 means it does not.
    /// </summary>
    private async Task<bool> ClassicQueueExistsAsync(IConnection connection, CancellationToken cancellationToken)
    {
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await channel.QueueDeclarePassiveAsync(_legacyName, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationInterruptedException e) when (e.ShutdownReason?.ReplyCode == 405)
        {
            return true;
        }
        catch (OperationInterruptedException e) when (e.ShutdownReason?.ReplyCode == 404)
        {
            return false;
        }
    }

    private async Task UnbindAsync(IConnection connection, CancellationToken cancellationToken)
    {
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await channel.QueueUnbindAsync(_sharedQueue, _legacyName, string.Empty,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (OperationInterruptedException e) when (e.ShutdownReason?.ReplyCode == 404)
        {
            // queue or exchange not there: nothing is bound
        }
    }

    private async Task<LegacyCommandBridgeState> BindAsync(IConnection connection, CancellationToken cancellationToken)
    {
        // The shared queue is declared by its own endpoint; never create it here with guessed arguments.
        await using (var probe = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
                         .ConfigureAwait(false))
        {
            try
            {
                await probe.QueueDeclarePassiveAsync(_sharedQueue, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationInterruptedException e) when (e.ShutdownReason?.ReplyCode == 404)
            {
                return LegacyCommandBridgeState.SharedQueueMissing;
            }
        }

        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            // Same arguments the classic consumer and the classic request client use
            // (MassTransit "exchange:N?temporary=true": fanout, non-durable, auto-delete).
            await channel.ExchangeDeclareAsync(_legacyName, ExchangeType.Fanout, durable: false, autoDelete: true,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await channel.QueueBindAsync(_sharedQueue, _legacyName, string.Empty,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return LegacyCommandBridgeState.Bound;
        }
        catch (OperationInterruptedException e) when (e.ShutdownReason?.ReplyCode == 404)
        {
            // the shared queue was removed between the probe and the bind (its last consumer left)
            return LegacyCommandBridgeState.SharedQueueMissing;
        }
        catch (OperationInterruptedException e) when (e.ShutdownReason?.ReplyCode == 406)
        {
            _logger.LogWarning(
                "Legacy command bridge '{SharedQueue}': the classic exchange '{Exchange}' exists with other " +
                "arguments ({Reason}); classic senders are not served by the shared queue until it is removed",
                _sharedQueue, _legacyName, e.ShutdownReason?.ReplyText);
            return LegacyCommandBridgeState.Faulted;
        }
    }

    private void LogTransition(LegacyCommandBridgeState from, LegacyCommandBridgeState to)
    {
        switch (to)
        {
            case LegacyCommandBridgeState.LegacyConsumerPresent:
                _logger.LogInformation(
                    "Legacy command bridge '{SharedQueue}': a classic exclusive consumer of '{Legacy}' is alive; " +
                    "this shared consumer serves '{Send}' only and stands by for classic senders until it is gone",
                    _sharedQueue, _legacyName, CacheCommon.GetSharedCommandSendExchangeName(_legacyName));
                break;
            case LegacyCommandBridgeState.Bound:
                _logger.LogInformation(
                    "Legacy command bridge '{SharedQueue}': no classic consumer of '{Legacy}'; the shared queue " +
                    "now also serves classic senders (was {From})", _sharedQueue, _legacyName, from);
                break;
            default:
                _logger.LogDebug("Legacy command bridge '{SharedQueue}': {From} -> {To}", _sharedQueue, from, to);
                break;
        }
    }

    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await ResetConnectionAsync().ConfigureAwait(false);

        var factory = new ConnectionFactory
        {
            HostName = _transportOptions.Host,
            Port = _transportOptions.Port,
            VirtualHost = string.IsNullOrEmpty(_transportOptions.VHost) ? "/" : _transportOptions.VHost,
            UserName = _transportOptions.User,
            Password = _transportOptions.Pass,
            ClientProvidedName = _clientName
        };
        if (_transportOptions.UseSsl)
        {
            factory.Ssl = new SslOption { Enabled = true, ServerName = _transportOptions.Host };
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ConnectTimeout);
        _connection = await factory.CreateConnectionAsync(timeout.Token).ConfigureAwait(false);
        return _connection;
    }

    private async Task ResetConnectionAsync()
    {
        var connection = _connection;
        _connection = null;
        if (connection == null)
        {
            return;
        }

        try
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // best effort
        }
    }

    /// <summary>
    ///     Stops the loop. The binding is deliberately left in place: the shared queue is shared by all replicas
    ///     and disappears with its last consumer.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _stopping.CancelAsync().ConfigureAwait(false);
        if (_loop != null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // expected
            }
        }

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            await ResetConnectionAsync().ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }

        _stopping.Dispose();
    }
}
