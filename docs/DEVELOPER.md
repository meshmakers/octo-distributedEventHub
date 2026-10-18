# Developer Documentation - DistributionEventHub

This documentation describes the implementation and usage of the DistributionEventHub for OctoMesh services.

## Note on MassTransit

This project uses **MassTransit v8.x**. MassTransit v9 has been commercialized and requires a paid license.

We are staying on v8 and monitoring the development of **OpenTransit** as a potential open-source alternative:
- [OpenTransit Documentation](https://opentransitlab.github.io/OpenTransit/docs/documentation.html)

---

## Table of Contents

1. [Installation and Configuration](#installation-and-configuration)
2. [Messaging Patterns](#messaging-patterns)
   - [Broadcast Events](#1-broadcast-events)
   - [Routed Events](#2-routed-events)
   - [Command Client (Request/Response)](#3-command-client-requestresponse)
3. [File Caching with MongoDB](#file-caching-with-mongodb)
4. [Message Scheduling](#message-scheduling)
5. [Exception Handling](#exception-handling)
6. [Configuration Options](#configuration-options)

---

## Installation and Configuration

### NuGet Packages

```xml
<PackageReference Include="Meshmakers.Octo.Common.DistributionEventHub" Version="3.3.*" />
<!-- Optional for MongoDB file caching -->
<PackageReference Include="Meshmakers.Octo.Common.DistributionEventHub.MongoDB" Version="3.3.*" />
```

### Basic Setup

```csharp
services.Configure<DistributionEventHubOptions>(options =>
    hostContext.Configuration.GetSection("DistributionEventHub").Bind(options));

services.AddDistributionEventHub(config =>
{
    // Required: Unique service address
    config.UniqueServiceAddress = "MyService";

    // Required: Instance prefix for environment isolation
    // Can also be set via appsettings.json
    config.InstancePrefix = "production";

    // Register consumers and clients here...
});
```

### appsettings.json Configuration

```json
{
  "DistributionEventHub": {
    "BrokerHost": "rabbitmq.example.com",
    "BrokerPort": 5672,
    "BrokerUser": "myuser",
    "BrokerPassword": "mypassword",
    "InstancePrefix": "production",
    "RepositoryHost": "mongodb.example.com",
    "SystemDatabaseName": "OctoSystem",
    "RepositoryUser": "octo-system-admin",
    "RepositoryPassword": "password",
    "RepositoryUseTls": true
  }
}
```

---

## Messaging Patterns

### 1. Broadcast Events

Broadcast events are sent to **all** registered consumer instances (fanout pattern).

**Use Cases:**
- Tenant reload notifications
- Global configuration changes
- Cache invalidation

#### Define Message

```csharp
public record TenantReloadEvent
{
    public string TenantId { get; init; } = null!;
}
```

#### Implement Consumer

```csharp
public class TenantReloadConsumer : IDistributedConsumer<TenantReloadEvent>
{
    private readonly ILogger<TenantReloadConsumer> _logger;

    public TenantReloadConsumer(ILogger<TenantReloadConsumer> logger)
    {
        _logger = logger;
    }

    public Task ConsumeAsync(IDistributedContext<TenantReloadEvent> context)
    {
        _logger.LogInformation("Tenant reload for: {TenantId}", context.Message.TenantId);

        // Business logic here...

        return Task.CompletedTask;
    }
}
```

#### Register Consumer

```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "MyService";
    config.AddBroadcastEventConsumer<TenantReloadConsumer, TenantReloadEvent>();
});
```

#### Publish Event

```csharp
public class MyService
{
    private readonly IDistributionEventHubService _eventHub;

    public MyService(IDistributionEventHubService eventHub)
    {
        _eventHub = eventHub;
    }

    public async Task NotifyTenantReload(string tenantId)
    {
        await _eventHub.PublishAsync(new TenantReloadEvent { TenantId = tenantId });
    }
}
```

---

### 2. Routed Events

Routed events are sent to **one** consumer instance (fire-and-forget with automatic load balancing).

**Use Cases:**
- Asynchronous job processing
- Background tasks
- Event routing to specific queues

#### Define Message

```csharp
public record ProcessDataEvent
{
    public string DataId { get; init; } = null!;
    public string Payload { get; init; } = null!;
}
```

#### Implement Consumer

```csharp
public class ProcessDataConsumer : IDistributedConsumer<ProcessDataEvent>
{
    private readonly ILogger<ProcessDataConsumer> _logger;

    public ProcessDataConsumer(ILogger<ProcessDataConsumer> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeAsync(IDistributedContext<ProcessDataEvent> context)
    {
        _logger.LogInformation("Processing data: {DataId}", context.Message.DataId);

        // Long-running processing...
        await Task.Delay(1000);

        // Optional: Publish another event
        await context.PublishAsync(new DataProcessedEvent { DataId = context.Message.DataId });
    }
}
```

#### Register Consumer

**With explicit queue address:**
```csharp
config.AddRoutedEventConsumer<ProcessDataConsumer, ProcessDataEvent>("data-processor");
```

**Without explicit address (uses message type as queue name):**
```csharp
config.AddRoutedEventConsumer<ProcessDataConsumer, ProcessDataEvent>();
```

#### Send Event

```csharp
public async Task SendProcessingJob(string dataId, string payload)
{
    var message = new ProcessDataEvent { DataId = dataId, Payload = payload };
    await _eventHub.SendAsync(new Uri("queue:data-processor"), message);
}
```

---

### 3. Command Client (Request/Response)

Command clients enable synchronous RPC communication with timeout and retry mechanisms.

**Use Cases:**
- Data queries from other services
- Operations requiring confirmation
- Service-to-service communication

#### Define Request and Response

```csharp
public record GetUserRequest
{
    public string UserId { get; init; } = null!;
}

public record GetUserResponse
{
    public string UserId { get; init; } = null!;
    public string Name { get; init; } = null!;
    public string Email { get; init; } = null!;
}
```

#### Implement Command Consumer

```csharp
public class GetUserCommandConsumer : IDistributedConsumer<GetUserRequest>
{
    private readonly IUserRepository _userRepository;
    private readonly ILogger<GetUserCommandConsumer> _logger;

    public GetUserCommandConsumer(IUserRepository userRepository, ILogger<GetUserCommandConsumer> logger)
    {
        _userRepository = userRepository;
        _logger = logger;
    }

    public async Task ConsumeAsync(IDistributedContext<GetUserRequest> context)
    {
        _logger.LogInformation("User request for: {UserId}", context.Message.UserId);

        var user = await _userRepository.GetByIdAsync(context.Message.UserId);

        // Send response
        await context.RespondAsync(new GetUserResponse
        {
            UserId = user.Id,
            Name = user.Name,
            Email = user.Email
        });
    }
}
```

#### Register Consumer and Client

**Service A (Consumer):**
```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "UserService";
    config.AddCommandConsumer<GetUserCommandConsumer, GetUserRequest>("get-user");
});
```

**Service B (Client):**
```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "ApiGateway";
    config.AddCommandClient<GetUserRequest>("get-user", timeout: TimeSpan.FromSeconds(30));
});
```

#### Call Command

**Standard call:**
```csharp
public class ApiController
{
    private readonly ICommandClient<GetUserRequest> _userClient;

    public ApiController(ICommandClient<GetUserRequest> userClient)
    {
        _userClient = userClient;
    }

    public async Task<GetUserResponse> GetUser(string userId)
    {
        var response = await _userClient.GetResponse<GetUserResponse>(
            new GetUserRequest { UserId = userId });

        return response;
    }
}
```

**With retry mechanism:**
```csharp
var response = await _userClient.GetResponseWithRetry<GetUserResponse>(
    new GetUserRequest { UserId = userId },
    retryCount: 3,
    retryWaitTime: TimeSpan.FromSeconds(5),
    timeout: TimeSpan.FromSeconds(10));
```

#### Routed Command Client (Dynamic Addressing)

For cases where the target is determined at runtime:

**Registration:**
```csharp
config.AddRoutedCommandClient<GetUserRequest>();
```

**Usage:**
```csharp
public class DynamicServiceCaller
{
    private readonly IRoutedCommandClient<GetUserRequest> _routedClient;

    public DynamicServiceCaller(IRoutedCommandClient<GetUserRequest> routedClient)
    {
        _routedClient = routedClient;
    }

    public async Task<GetUserResponse> GetUserFromService(string serviceAddress, string userId)
    {
        return await _routedClient.GetResponse<GetUserResponse>(
            serviceAddress,
            new GetUserRequest { UserId = userId });
    }

    public async Task<GetUserResponse> GetUserWithRetry(string serviceAddress, string userId)
    {
        return await _routedClient.GetResponseWithRetry<GetUserResponse>(
            serviceAddress,
            new GetUserRequest { UserId = userId },
            retryCount: 5,
            retryWaitTime: TimeSpan.FromSeconds(30));
    }
}
```

#### Shared Command Endpoints (Competing Consumers, `mandatory` Send)

Used by CK v2 methods (`Meshmakers.Octo.Services.Methods`, AB#5671). A dynamic command endpoint registered with
`CommandConsumerOptions` is a **shared** endpoint: any number of processes can register the same command name and
become competing consumers on one queue. It is called with the `CommandSendOptions` overload of
`GetCommandResponseAsync`. **The two options overloads form a pair** — do not mix them with the legacy
`RegisterCommandConsumer(name, handler)` / `GetCommandResponseAsync(..., timeout)` overloads for the same command
name (the broker topology differs and declarations would conflict).

```csharp
// Handler side (bus already started)
EndpointHandle handle = eventHubControl.RegisterCommandConsumer<MyRequest>(
    "octo::method::system.identity::user::changepassword-1",
    async (request, respond) =>
    {
        try { await respond(await HandleAsync(request)); }
        catch (Exception e) { await respond(MyResponse.Failed(e)); } // never let an exception escape (see below)
    },
    new CommandConsumerOptions { PrefetchCount = 16 });      // ConfigureConsumeTopology defaults to false
await handle.WaitUntilReadyAsync();                          // queue + bindings exist → announce now
...
await handle.DisposeAsync();                                 // removes the consumer; queue is auto-deleted

// Caller side
try
{
    var response = await eventHub.GetCommandResponseAsync<MyRequest, MyResponse>(
        "octo::method::system.identity::user::changepassword-1", request,
        new CommandSendOptions { Timeout = timeout, Mandatory = true, TimeToLive = timeout },
        cancellationToken);
}
catch (CommandNotRoutableException) { /* no consumer registered: returned by the broker in ~1 ms */ }
catch (DistributionTimeoutException) { /* consumer did not answer in time */ }
catch (DistributedOperationFailedException) { /* consumer faulted / send failed */ }
// OperationCanceledException from the caller's token is re-thrown unchanged.
```

**Broker topology** (all names carry the instance prefix):

| Entity | Name | Flags | Why |
|---|---|---|---|
| Queue | `{prefix}-{commandName}` | durable, auto-delete, **non-exclusive** | MassTransit forces `exclusive` on every non-durable auto-delete queue (`BrokerTopologyBuilder.QueueDeclare`), so a second process would fail with `RESOURCE_LOCKED` and its endpoint would never become ready. Durable + auto-delete is non-exclusive and is still removed when the last consumer leaves (graceful stop, or broker-detected dead connection). |
| Send exchange | `{prefix}-{commandName}::send` (fanout) | durable, **not** auto-delete | Stable target for senders. The endpoint's own exchange is auto-deleted together with the queue; publishing to a deleted exchange closes the channel (404) and the next send took ~6 s to recover. A `mandatory` send to the stable exchange with no bound queue is returned in ~1 ms. |
| Endpoint exchange | `{prefix}-{commandName}` | durable, auto-delete | MassTransit's per-endpoint exchange; not used by senders. |

**`mandatory` and publisher confirms.** MassTransit 8.5.10 enables RabbitMQ publisher confirmation by default
(`ConfigurationHostSettings.PublisherConfirmation = true`), which is what surfaces a `basic.return` as an exception.
No bus configuration change is needed. Measured on a local RabbitMQ container: round trip p50 0.91 ms (non-mandatory)
vs 0.95 ms (mandatory); return without a bound queue 0–2 ms (integration test `CommandSendOptionsTests`).

**Retry.** The bus-level `UseMessageRetry(1s, 5s, 15s)` also wraps dynamically connected endpoints: a handler that
throws is executed **4 times** (proven by `ThrowingDynamicConsumer_IsRetriedByBus_FourExecutions`). Handlers that must
run at most once (CK methods) catch everything and respond with an error instead of throwing.

**Delivery information (duplicate detection).** The overload taking an `ExecuteDeliveredCommandHandler<TMessage>`
`(message, CommandDeliveryInfo delivery, respond)` exposes `Redelivered` (the broker delivered the message before
without an ack — consumer connection lost, process killed, broker restart; the earlier delivery may or may not have
been processed), `RetryAttempt` (bus retry filter) and `RedeliveryCount`. `delivery.IsPossibleDuplicate` is `true` if
any of them indicates an earlier attempt. Non-idempotent handlers must not execute such a message again (CK methods
answer `METHOD_OUTCOME_UNKNOWN`). Proven by `CommandDeliveryTests` (a raw consumer takes the message without ack, the
broker redelivers it with `redelivered = true`).

**Sensitive payloads.**
- `CommandSendOptions.Persistent = false` sends a transient message (`delivery_mode = 1`), so it is not written to the
  persistent message store. Residual risk: RabbitMQ can still page transient messages to disk under memory pressure
  (classic queue v2 paging); keep `TimeToLive` short.
- `CommandConsumerOptions.SuppressFaults = true`: exceptions escaping the handler are logged without the message and
  swallowed (no bus retry, no `Fault<TMessage>` — which embeds the full message — to the requester or as a publish,
  requester runs into its timeout); faulted messages (e.g. undeserializable bodies) and skipped messages are
  **discarded** instead of being moved to the never-consumed `{queue}_error` / `{queue}_skipped` queues
  (`DiscardFaultedMessages`, `DiscardSkippedMessages`, `PublishFaults = false`).
- The queue itself must be durable (non-exclusive, see above); with transient messages that only means the queue
  definition survives a broker restart, not the messages.

**Cancellation.** The options send overload re-throws `OperationCanceledException` only for the caller's token; a
cancellation from elsewhere (e.g. the bus is stopping) is wrapped in `DistributedOperationFailedException`.

**Logging of mandatory returns.** MassTransit itself logs a broker return as `ERROR SendTransport: S-FAULT …::send`
(category `MassTransit`). It is an expected outcome for CK methods (no handler); filter or lower that category in the
host's logging configuration if the noise matters. Callers get `CommandNotRoutableException`.

**Legacy endpoints are exclusive.** The same MassTransit rule makes the classic `AddCommandConsumer(name)` endpoints
(`Temporary = true`) and the legacy dynamic `RegisterCommandConsumer(name, handler)` queues **exclusive** at the broker:
only one process consumes a given command name at a time; other replicas retry the declaration until the owner's
connection is gone. This is deprecated since AB#6909 (one warning per command name and process); see the next section
for the migration to a shared queue.

#### Migrating exclusive command queues (AB#6909)

**Opt-in per consumer.** Nothing changes for a registration without the switch: queue name, arguments and behaviour are
byte-identical to before (pinned by `LegacyCommandEndpointUnchangedTests` and
`LegacyCommandMigrationTests.ClassicRegistration_WithoutOption_KeepsExclusiveQueue_NoSharedQueueNoSendExchange`).

```csharp
// DI registration (service Program.cs)
config.AddCommandConsumer<ImportCkConsumer, ImportCkCommandRequest>("octo::bot::import-ck",
    new CommandConsumerOptions { MigratesLegacyCommand = true });

// dynamic registration
EndpointHandle handle = eventHubControl.RegisterCommandConsumer<MyRequest>("my-command", handler,
    new CommandConsumerOptions { MigratesLegacyCommand = true });
await handle.WaitUntilReadyAsync();
```

Senders (`AddCommandClient`, `ICommandClient`, `GetCommandResponseAsync(..., timeout)`) are **not changed**.

**Topology decision (and why).** RabbitMQ refuses to redeclare a queue or an exchange with different arguments
(`PRECONDITION_FAILED`, 406), and an exclusive queue cannot be used by any other connection (`RESOURCE_LOCKED`, 405).
The classic queue/exchange `N` is non-durable/exclusive; the shared topology needs a durable, non-exclusive queue. Both on
the name `N` would make the old and the new replica fail on each other, in either start order. A migrated command therefore
uses its own names and never touches `N`:

| Entity | Name | Flags | Used by |
|---|---|---|---|
| Queue | `{prefix}-{commandName}::shared` | durable, auto-delete, non-exclusive | migrated replicas (competing consumers) |
| Send exchange | `{prefix}-{commandName}::send` (fanout) | durable, not auto-delete | `CommandSendOptions` senders |
| Classic exchange | `{prefix}-{commandName}` (fanout) | non-durable, auto-delete | classic senders; bound to the shared queue only by the bridge, only while no classic consumer exists |

**The bridge: no loss, no double processing without a flag day.** A fanout exchange copies a message into every bound
queue. If the shared queue were bound to the classic exchange permanently, every classic command would be processed twice
while an old replica runs. The migrated replicas therefore reconcile every `LegacyProbeInterval` (default 2 s):
the classic queue alive (passive declare answers 405 or succeeds) -> unbind and stand by; gone (404) -> bind. While an old
replica exists it receives all classic commands (as before); as soon as the last one is gone, the migrated replicas serve them.
All replicas run the same idempotent check, so they converge. The bridge never creates the shared queue and does not remove its
binding on shutdown (the queue is shared and disappears with its last consumer).

**Rollout order** (per service, no order between services, senders untouched):

1. Release the library containing this change; no service behaviour changes.
2. Set `MigratesLegacyCommand = true` on the registrations of one service (bot-services and identity: AB#6910). Make sure the
   handler is safe under competing consumers.
3. Roll the service (rolling update). Old replicas keep the classic traffic; new replicas start cleanly and stand by; after the
   last old replica terminated, the new replicas take over within one probe interval and share the work.
4. Optionally move senders to `CommandSendOptions` (mandatory send) later, when every consumer of the command is migrated.
5. Rollback: stop the migrated replicas first, then start the old version.

**What can be lost (honest window).** (a) Commands that are *ready in the old queue* when the last old replica stops vanish
with that non-durable queue; delivered commands are completed first. This is identical to a restart of the former single
consumer. (b) Between the stop of the last old replica and the next bridge pass (<= `LegacyProbeInterval`) classic commands
reach no queue; the sender's timeout/retry covers it. (c) In a rollback where an old replica starts while the shared queue is
bound, commands are processed twice for <= one interval. `LegacyCommandMigrationTests` proves: no command is processed twice
in the rolling-update scenario, no command is processed twice after the bridge stepped aside in a rollback, and everything
sent after the take-over is processed exactly once.

**Failure behaviour of the new replica.**

| Situation | Reaction | Test |
|---|---|---|
| Classic queue held exclusively by an old replica | starts normally, stands by, INFO log | `QueueDeclaredExclusiveByForeignConnection_SharedReplicaStartsAndReactsWithoutPreconditionFailed` |
| Classic exchange exists with other arguments (for example durable) | bridge `Faulted`, WARN log; the shared queue still serves `::send`; recovers when the exchange is removed | `ClassicExchangeWithOtherArguments_FaultsWithoutThrowing_AndRecoversWhenItIsRemoved` |
| Broker unreachable for the bridge | bridge `Faulted`, retries every interval, never throws | `BrokerUnreachable_ReconcileFaultsWithoutThrowing` |
| Shared queue not declared yet | `SharedQueueMissing`, retried; the bridge never creates the queue | `SharedQueueDoesNotExistYet_ReportsMissing_AndDoesNotCreateIt` |

**Deprecation.** The classic registrations log one warning per command name and process. No `[Obsolete]` attribute: all consumer
repositories build with warnings as errors, and consumers that are exclusive *by design* (one instance owns the address, for
example per-pipeline command nodes) must keep compiling. The consumer list is in `CLAUDE.md`.

**Operations.** New broker names appear (`*::shared` queues, `*::send` exchanges). Monitoring or broker policies that match
command queue names must include them; the classic queue disappears from a migrated service as soon as its last old replica
is gone.

---

## File Caching with MongoDB

The MongoDB module provides GridFS-based file caching with TTL support.

### Setup

```csharp
services.AddDistributionEventHub(config => { /* ... */ })
    .AddMongoDbRepository();
```

### Usage

```csharp
public class ReportService
{
    private readonly IDistributedCacheService _cacheService;

    public ReportService(IDistributedCacheService cacheService)
    {
        _cacheService = cacheService;
    }

    // Cache file with TTL
    public async Task<string> CacheReport(string tenantId, Stream reportStream)
    {
        var cacheKey = await _cacheService.CreateStreamAsync(
            tenantId,
            reportStream,
            "application/pdf",
            "report.pdf",
            expiry: TimeSpan.FromHours(24));

        return cacheKey;
    }

    // Cache file without TTL (unlimited validity, overwritten on re-upload)
    public async Task<string> CachePermanentFile(string tenantId, Stream fileStream)
    {
        return await _cacheService.CreateOrUpdateStreamAsync(
            tenantId,
            fileStream,
            "application/json",
            "config.json");
    }

    // Retrieve file
    public async Task<CacheStream?> GetReport(string tenantId, string cacheKey)
    {
        return await _cacheService.GetCacheStreamByIdAsync(tenantId, cacheKey);
    }

    // Retrieve file by filename
    public async Task<CacheStream?> GetReportByFileName(string tenantId, string fileName)
    {
        return await _cacheService.GetCacheStreamByFileNameAsync(tenantId, fileName);
    }

    // Delete single file
    public async Task DeleteReport(string tenantId, string cacheKey)
    {
        await _cacheService.DeleteCacheStreamAsync(tenantId, cacheKey);
    }

    // Delete all expired files for a tenant
    public async Task CleanupExpiredFiles(string tenantId)
    {
        await _cacheService.DeleteAllExpiredCacheStreamsAsync(tenantId);
    }

    // Delete all files for a tenant
    public async Task DeleteAllFiles(string tenantId)
    {
        await _cacheService.DeleteAllCacheStreamsAsync(tenantId);
    }
}
```

---

## Message Scheduling

Hangfire is supported for time-based message scheduling.

### Setup

```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "SchedulerService";
    config.AddHangfireMessageScheduler();

    // Optional: Customize scheduler endpoint
    config.SchedulerEndpointAddress = "queue:my-scheduler";
});
```

### Create Recurring Schedule

```csharp
public class ScheduledJobService
{
    private readonly IDistributionEventHubService _eventHub;

    public ScheduledJobService(IDistributionEventHubService eventHub)
    {
        _eventHub = eventHub;
    }

    public async Task ScheduleDailyReport()
    {
        var options = new RecurringSchedulingOptions(
            cronExpression: "0 8 * * *",  // Daily at 8:00 AM
            scheduleId: "daily-report",
            scheduleGroup: "reports",
            description: "Daily report generation",
            misfirePolicy: SchedulingMissedEventPolicy.Default);

        await _eventHub.ScheduleRecurringSendAsync(
            new GenerateReportEvent { ReportType = "daily" },
            destinationQueueAddress: "report-processor",
            options);
    }

    public async Task ScheduleInUserTimeZone()
    {
        // Fires at 07:00 in Vienna, daylight saving included. Without TimeZoneId the
        // cron expression is evaluated in the local zone of the process that creates
        // the schedule.
        var options = new RecurringSchedulingOptions(
            cronExpression: "0 0 7 * * ?",
            scheduleId: "morning-digest",
            scheduleGroup: "digests",
            description: "Morning digest")
        {
            TimeZoneId = "Europe/Vienna"
        };

        await _eventHub.ScheduleRecurringSendAsync(
            new GenerateReportEvent { ReportType = "digest" },
            destinationQueueAddress: "report-processor",
            options);
    }

    public async Task ScheduleWithTimeRange()
    {
        var options = new RecurringSchedulingOptions(
            cronExpression: "0 */2 * * *",  // Every 2 hours
            startTime: DateTime.Now,
            endTime: DateTime.Now.AddMonths(1),  // Only for one month
            scheduleId: "hourly-sync",
            scheduleGroup: "sync",
            description: "Bi-hourly synchronization");

        await _eventHub.ScheduleRecurringSendAsync(
            new SyncEvent(),
            destinationQueueAddress: "sync-processor",
            options);
    }

    public async Task CancelScheduledJob()
    {
        await _eventHub.CancelScheduledRecurringSendAsync(
            scheduleId: "daily-report",
            scheduleGroup: "reports");
    }
}
```

### Consuming recurring ticks: latest-only consumers (AB#5709)

A recurring send is delivered into a **durable** queue. While its consumer is down, the ticks pile up,
and a plain consumer then handles all of them at once — up to MassTransit's default prefetch
(processor count × 2) in parallel. For periodic work (cron pipeline triggers) a backlog of N ticks is
worth exactly one run. Register such consumers at runtime with `RoutedEventConsumerOptions.LatestOnly`:

```csharp
var handle = eventHubControl.RegisterRoutedEventConsumer<PipelineTriggerSchedule>(queueName,
    async (tick, delivery) =>
    {
        // delivery.CoalescedMessageCount = older ticks that were skipped in favour of this one
        await RunAsync(tick);
    },
    RoutedEventConsumerOptions.LatestOnly);
```

`LatestOnly` = `PrefetchCount = 1`, `ConcurrentMessageLimit = 1`, `CoalescePendingMessages = true`:

- one message at a time — the handler never runs concurrently with itself on this endpoint;
- before invoking the handler, the queue is passively declared on the consuming channel; if newer
  messages are already waiting (`message-count > 0`), the delivery is acknowledged without invoking the
  handler. A backlog of 20 ⇒ 19 skips + 1 handled message (the newest), and ticks that arrive while the
  handler runs collapse into one follow-up run — the semantics of `x-max-length = 1` +
  `x-overflow = drop-head`;
- if the count cannot be read (non-RabbitMQ transport, broker error) the message is handled (fail open).

**No queue arguments are involved.** Prefetch is channel QoS and the rest is decided in-process, so the
options apply to existing durable queues as they are — no redeclare, no `PRECONDITION_FAILED`, no queue
migration. (Producers that send to `queue:<name>` declare the queue too, without arguments; adding
`x-max-length`/`x-message-ttl` as queue arguments on one side only would break the other.)

Limits: the guarantee is per receive endpoint, i.e. per consumer process. Several processes consuming the
same queue each run one message at a time. `CoalescePendingMessages` requires a prefetch count and
concurrency limit of 1 (validated; `ArgumentException` otherwise) because prefetched messages are not
counted as waiting by the broker. With N competing consumers the tail of a backlog can therefore still
produce up to N runs (one per consumer, in parallel), e.g. briefly during a rolling update.

Further notes from the AB#5709 review:

- `message-count` of `queue.declare-ok` is the number of **ready** messages; the delivery being decided
  on is unacknowledged and not included — for classic and for quorum queues (covered by
  `BacklogInAQuorumQueue_IsHandledOnceWithTheNewestTick`). Since both sides declare the queue without
  arguments, a quorum queue only arises from the vhost's default queue type. For quorum queues the count
  comes from the leader's state and could in theory be stale right after a leader change; the worst case
  is one skipped tick that the next tick makes up for.
- A delivery is only skipped while a newer message is still in the queue, and nothing is acknowledged
  before the handler finishes, so a crash never loses the last tick: the unacknowledged delivery is
  requeued and coalesced against whatever is waiting when it is redelivered (at worst one extra run).
- The count is read through MassTransit's RabbitMQ payloads (`ChannelContext`, `ReceiveSettings`). If a
  MassTransit upgrade removes them, coalescing silently falls back to handling every message (still one
  at a time). `RoutedEventLatestOnlyTests` (RabbitMQ Testcontainer) is the guard for such upgrades.
- A passive declare of a missing queue closes the channel (404). The queue being consumed always exists,
  unless it is deleted while the consumer runs — then RabbitMQ cancels the consumer anyway and MassTransit
  recovers the channel and endpoint.

Note on `SchedulingMissedEventPolicy`: MassTransit.Hangfire 8.5.x registers recurring jobs with only a
time zone (`RecurringJobOptions { TimeZone }`); the misfire policy is not passed to Hangfire. It would only
concern ticks missed while the **scheduler** is down anyway — not ticks buffered while the consumer is down.

---

## Exception Handling

### Exception Types

| Exception | Description |
|-----------|-------------|
| `DistributionException` | Base exception for all distribution errors |
| `DistributionTimeoutException` | Request timeout on command calls |
| `DistributedOperationFailedException` | General operation failures |
| `CommandNotRoutableException` | A `mandatory` command send was returned by the broker (no queue bound / no consumer registered) |

### Error Handling

```csharp
public async Task<GetUserResponse?> GetUserSafe(string userId)
{
    try
    {
        return await _userClient.GetResponse<GetUserResponse>(
            new GetUserRequest { UserId = userId },
            timeout: TimeSpan.FromSeconds(10));
    }
    catch (DistributionTimeoutException ex)
    {
        _logger.LogWarning(ex, "Timeout on user query for {UserId}", userId);
        return null;
    }
    catch (DistributedOperationFailedException ex)
    {
        _logger.LogError(ex, "Error on user query for {UserId}", userId);
        throw;
    }
}
```

---

## Configuration Options

### IDistributionEventHubConfiguration

| Property | Type | Description |
|----------|------|-------------|
| `UniqueServiceAddress` | `string` | **Required.** Unique name of the service |
| `InstancePrefix` | `string` | **Required.** Prefix for queue/exchange isolation |
| `AutomaticallyStartBusDuringStartup` | `bool` | Auto-start bus (Default: `true`) |
| `SchedulerEndpointAddress` | `string` | Scheduler queue address (Default: `queue:scheduler`) |

### DistributionEventHubOptions (appsettings.json)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `InstancePrefix` | `string?` | - | Instance prefix (overridable in code) |
| `BrokerHost` | `string` | `localhost` | RabbitMQ host |
| `BrokerPort` | `ushort` | `5672` | RabbitMQ port |
| `BrokerUser` | `string?` | - | RabbitMQ username |
| `BrokerPassword` | `string?` | - | RabbitMQ password |
| `RepositoryHost` | `string` | `localhost` | MongoDB host |
| `SystemDatabaseName` | `string` | `OctoSystem` | MongoDB database name |
| `RepositoryUser` | `string` | `octo-system-admin` | MongoDB username |
| `RepositoryPassword` | `string?` | - | MongoDB password |
| `RepositoryUseTls` | `bool` | `true` | TLS for MongoDB |
| `RepositoryAllowInsecureTls` | `bool` | `false` | Allow insecure TLS |
| `DatabaseAuthenticationSource` | `string` | `admin` | MongoDB auth database |

### Manual Bus Start/Stop

```csharp
public class StartupService : IHostedService
{
    private readonly IEventHubControl _eventHubControl;

    public StartupService(IEventHubControl eventHubControl)
    {
        _eventHubControl = eventHubControl;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Perform initialization...

        // Start bus manually
        await _eventHubControl.StartAsync(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _eventHubControl.StopAsync(cancellationToken);
    }
}
```

**Configuration for manual start:**
```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "MyService";
    config.AutomaticallyStartBusDuringStartup = false;  // Manual mode
});
```

---

## IDistributedContext API

The `IDistributedContext<TMessage>` provides access to the received message and enables interactions:

| Member | Description |
|--------|-------------|
| `Message` | The received message |
| `RespondAsync<T>(T message)` | Send response for request/response pattern |
| `PublishAsync<T>(T message)` | Publish broadcast event from consumer |

---

## Queue Naming Schema

The system uses the following naming schema for queues and exchanges:

| Pattern | Queue Name |
|---------|------------|
| Broadcast | `{InstancePrefix}:octo::service::{ServiceName}-{InstanceId}` |
| Routed | `{InstancePrefix}:{DestinationAddress}` |
| Command | `{InstancePrefix}:{CommandName}` |
| Shared command (options overloads) | queue `{InstancePrefix}-{CommandName}`, send exchange `{InstancePrefix}-{CommandName}::send` |

Example with `InstancePrefix = "prod"`:
- Broadcast: `prod:octo::service::UserService-abc123`
- Routed: `prod:data-processor`
- Command: `prod:get-user`
