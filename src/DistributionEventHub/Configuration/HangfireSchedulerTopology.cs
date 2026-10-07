using MassTransit;
using MassTransit.Scheduling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Meshmakers.Octo.Common.DistributionEventHub.Configuration;

/// <summary>
///     Names of the Hangfire scheduler endpoint (AB#5867).
/// </summary>
/// <remarks>
///     <para>
///         🔴 MassTransit's <c>HangfireEndpointDefinition</c> returns <c>HangfireEndpointOptions.QueueName</c>
///         verbatim — the endpoint name formatter, and with it the instance prefix, is never applied. The
///         queue was therefore called <c>hangfire</c> in EVERY OctoMesh instance, while the message
///         exchanges bound into it are instance-prefixed (<c>dev-MassTransit.Scheduling:ScheduleRecurringMessage</c>).
///         On a broker shared by two instances (test-2: <c>main</c> and <c>dev</c>) both bot services consumed
///         the same queue as competing consumers, so a recurring schedule of one instance was stored by
///         whichever bot service picked it up — in that instance's Hangfire database. The job still fired
///         into the right instance (the destination address is absolute), but
///         <c>RemoveRecurringJobsByScheduleGroup</c> is a prefixed command and only reaches the own bot
///         service: a schedule that landed in the foreign database could never be removed again. Seen on
///         test-2-dev 2026-10-07: two cron ticks per interval after a tenant update, one left after
///         <c>UndeployTriggers</c>.
///     </para>
/// </remarks>
internal static class HangfireSchedulerTopology
{
    /// <summary>
    ///     The queue (and exchange) name every instance used before AB#5867 and MassTransit's default.
    /// </summary>
    internal const string LegacyQueueName = "hangfire";

    /// <summary>
    ///     The instance-scoped scheduler queue, e.g. <c>dev-hangfire</c>.
    /// </summary>
    internal static string GetQueueName(string instancePrefix) =>
        CacheCommon.ApplyInstancePrefix(instancePrefix, LegacyQueueName);
}

/// <summary>
///     Removes this instance's scheduler message exchanges from the legacy, un-prefixed <c>hangfire</c>
///     exchange before the bus starts (AB#5867).
/// </summary>
/// <remarks>
///     <para>
///         Renaming the endpoint alone is not enough: RabbitMQ keeps the bindings the old version created,
///         so <c>dev-MassTransit.Scheduling:ScheduleRecurringMessage → hangfire</c> would survive and every
///         schedule of this instance would ALSO be copied into the legacy queue — where a not yet updated
///         bot service of another instance keeps consuming it. That would turn today's random split into
///         a guaranteed duplicate. Only this instance's own bindings are removed; another instance's
///         bindings and the legacy queue itself stay untouched, so an instance that has not been updated
///         keeps working exactly as before.
///     </para>
///     <para>
///         Best effort by design: a missing exchange or binding is the normal case on a fresh broker, and
///         a broker error must never keep the scheduler from starting — it is logged and the start goes on.
///     </para>
/// </remarks>
internal sealed class LegacyHangfireSchedulerBindingRemover(
    IOptions<RabbitMqTransportOptions> transportOptions,
    Func<string> instancePrefix,
    ILogger<LegacyHangfireSchedulerBindingRemover> logger) : IBusObserver
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    public void PostCreate(IBus bus)
    {
    }

    public void CreateFaulted(Exception exception)
    {
    }

    public async Task PreStart(IBus bus)
    {
        var sourceExchanges = SchedulingSourceExchanges(bus);
        await RemoveLegacyBindingsAsync(sourceExchanges).ConfigureAwait(false);
    }

    public Task PostStart(IBus bus, Task<BusReady> busReady) => Task.CompletedTask;

    public Task StartFaulted(IBus bus, Exception exception) => Task.CompletedTask;

    public Task PreStop(IBus bus) => Task.CompletedTask;

    public Task PostStop(IBus bus) => Task.CompletedTask;

    public Task StopFaulted(IBus bus, Exception exception) => Task.CompletedTask;

    internal static IReadOnlyList<string> SchedulingSourceExchanges(IBus bus) =>
    [
        Name<ScheduleMessage>(bus),
        Name<ScheduleRecurringMessage>(bus),
        Name<CancelScheduledMessage>(bus),
        Name<CancelScheduledRecurringMessage>(bus),
        Name<PauseScheduledRecurringMessage>(bus),
        Name<ResumeScheduledRecurringMessage>(bus)
    ];

    private static string Name<T>(IBus bus) where T : class =>
        bus.Topology.Message<T>().EntityNameFormatter.FormatEntityName();

    /// <summary>
    ///     Unbinds each of <paramref name="sourceExchanges" /> from the legacy <c>hangfire</c> exchange.
    ///     Returns the number of unbind calls the broker accepted (an absent binding counts as accepted).
    /// </summary>
    internal async Task<int> RemoveLegacyBindingsAsync(IReadOnlyList<string> sourceExchanges)
    {
        var prefix = instancePrefix();
        var options = transportOptions.Value;
        var accepted = 0;

        using var cts = new CancellationTokenSource(Timeout);
        try
        {
            var factory = new ConnectionFactory
            {
                HostName = options.Host,
                Port = options.Port,
                VirtualHost = string.IsNullOrEmpty(options.VHost) ? "/" : options.VHost,
                UserName = options.User,
                Password = options.Pass,
                ClientProvidedName = $"{prefix}-hangfire-legacy-binding-cleanup"
            };
            if (options.UseSsl)
            {
                factory.Ssl = new SslOption { Enabled = true, ServerName = options.Host };
            }

            await using var connection = await factory.CreateConnectionAsync(cts.Token).ConfigureAwait(false);
            foreach (var source in sourceExchanges)
            {
                // One channel per call: a 404 (source exchange or legacy exchange absent) closes the
                // channel, and the remaining unbinds must still run.
                try
                {
                    await using var channel = await connection.CreateChannelAsync(cancellationToken: cts.Token)
                        .ConfigureAwait(false);
                    await channel.ExchangeUnbindAsync(HangfireSchedulerTopology.LegacyQueueName, source,
                        string.Empty, cancellationToken: cts.Token).ConfigureAwait(false);
                    accepted++;
                }
                catch (OperationInterruptedException e)
                {
                    logger.LogDebug(
                        "No legacy scheduler binding '{Source}' -> '{Legacy}' to remove: {Reason}",
                        source, HangfireSchedulerTopology.LegacyQueueName, e.ShutdownReason?.ReplyText);
                }
            }

            logger.LogInformation(
                "Instance '{Prefix}': scheduler uses queue '{Queue}'; legacy bindings of this instance into the " +
                "shared '{Legacy}' exchange removed ({Accepted}/{Total} unbind calls accepted) (AB#5867)",
                prefix, HangfireSchedulerTopology.GetQueueName(prefix), HangfireSchedulerTopology.LegacyQueueName,
                accepted, sourceExchanges.Count);
        }
        catch (Exception e)
        {
            logger.LogWarning(e,
                "Instance '{Prefix}': could not remove the legacy scheduler bindings into the shared '{Legacy}' " +
                "exchange. If another OctoMesh instance shares this broker, its bot service may still store this " +
                "instance's schedules (AB#5867); remove the bindings '{Prefix}-MassTransit.Scheduling:* -> {Legacy}' " +
                "by hand",
                prefix, HangfireSchedulerTopology.LegacyQueueName, prefix, HangfireSchedulerTopology.LegacyQueueName);
        }

        return accepted;
    }
}
