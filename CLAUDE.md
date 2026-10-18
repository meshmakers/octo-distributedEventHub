# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build with local dependencies (DebugL configuration) - the default for local work
dotnet build Octo.DistributedEventHub.sln -c DebugL

# Build in Release mode against a published release (version must be passed explicitly)
dotnet build Octo.DistributedEventHub.sln -c Release -p:OctoVersion=3.5.1
```

Versions come only from the pipeline (AB#6291): `Directory.Build.props` has no release fallback, so a
Debug/Release build without `-p:OctoVersion=X.Y.Z` (or `-p:OctoNugetPrivateServer=<feed>` for the main
line `0.1.*`) fails fast with MSBuild error `OCTO0001`. Use `-c DebugL` locally.

Unit tests: `tests/DistributionEventHub.UnitTests`; integration tests (Testcontainers RabbitMQ/MongoDB, Docker required): `tests/DistributionEventHub.IntegrationTests`.

CI (`azure-pipelines.yml`): `Build src and tests` builds `src/**` plus the `*Tests` projects once; `Test` then runs `dotnet test --no-build --no-restore` per project, sequentially (AB#6093). A new test project outside `tests/**/*Tests.csproj` must be added to the build step's project list.

```bash
# Unit tests
dotnet test tests/DistributionEventHub.UnitTests -c DebugL

# Integration tests (Testcontainers: start their own RabbitMQ / MongoDB containers, Docker required)
dotnet test tests/DistributionEventHub.IntegrationTests -c DebugL
```

Set `OCTO_DEH_TEST_LOG=/path/file.log` to get the timing diagnostics of `CommandSendOptionsTests`
(mandatory-return latency, round-trip p50/p95) written to a file.

## Architecture Overview

This is a distributed event system for OctoMesh services built on **MassTransit** with **RabbitMQ** transport. The library provides type-safe APIs for four core messaging patterns.

### Project Structure

- `src/DistributionEventHub` - Core library (NuGet: `Meshmakers.Octo.Common.DistributionEventHub`)
  - Targets `net10.0` only
- `src/DistributionEventHub.MongoDB` - MongoDB GridFS integration for file caching
- `samples/` - Example services demonstrating usage patterns

### Core Messaging Patterns

1. **Broadcast Events** - Fanout to all services via `PublishAsync<T>()`. Queue pattern: `octo::service::{ServiceName}`
2. **Routed Events** - Fire-and-forget to specific queues via `SendAsync<T>(Uri, message)` with round-robin load balancing
3. **CommandClient** - Request/response RPC via `ICommandClient<TRequest>` and `IRoutedCommandClient<TRequest>`
4. **Distributed Cache** - MongoDB GridFS file caching via `IDistributedCacheService`

Recurring sends (`ScheduleRecurringSendAsync` with `RecurringSchedulingOptions`) evaluate the cron in
`RecurringSchedulingOptions.TimeZoneId` (IANA or Windows id). When it is null or empty, the local zone of the process that
creates the schedule is used (usually UTC in a container). Set it whenever the cron names a time of day for a person (the
notification schedule passes each recipient's zone). Usage: `docs/DEVELOPER.md`; tests: `OctoRecurringScheduleTests`.

### Shared Command Endpoints (CK methods, AB#5671)

`IEventHubControl.RegisterCommandConsumer(name, handler, CommandConsumerOptions)` and
`IDistributionEventHubService.GetCommandResponseAsync(name, request, CommandSendOptions, ct)` are a **pair**:
competing consumers on a durable auto-delete, non-exclusive queue, senders publish to the stable exchange
`{prefix}-{name}::send`. `CommandSendOptions.Mandatory` → `CommandNotRoutableException` in ~1 ms when no consumer is
bound. Await `EndpointHandle.WaitUntilReadyAsync()` before announcing an endpoint. Gotchas (see `docs/DEVELOPER.md`):
MassTransit forces `exclusive` on non-durable auto-delete queues (so legacy command endpoints are single-consumer),
publisher confirms are on by default in MassTransit 8.5.10, and the bus-level retry (1s/5s/15s) also re-executes
throwing dynamic handlers (4 executions) — method handlers must never throw.
Review fixes (H5/M5/L7): `ExecuteDeliveredCommandHandler` overload with `CommandDeliveryInfo`
(`Redelivered`, `RetryAttempt`, `IsPossibleDuplicate`) so non-idempotent handlers can refuse redeliveries;
`CommandSendOptions.Persistent = false` (transient messages) and `CommandConsumerOptions.SuppressFaults` (no retry, no
`Fault<T>`, no `_error`/`_skipped` copies) for sensitive payloads; a non-caller `OperationCanceledException` maps to
`DistributedOperationFailedException`.

Opt-in (AB#6891): the shared-endpoint overloads are additive. `AddCommandConsumer`, the plain
`RegisterCommandConsumer(name, handler)` and the plain `GetCommandResponseAsync` keep the exclusive, non-durable
auto-delete queue and the `?temporary=true` request exchange (pinned by
`IntegrationTests/Messaging/LegacyCommandEndpointUnchangedTests`). The new interface members have default
implementations that throw `NotSupportedException`, so existing test doubles of `IEventHubControl` /
`IDistributionEventHubService` keep compiling. Migrating the existing exclusive command queues to shared endpoints
is a separate story. A send right after `RegisterCommandConsumer` without `WaitUntilReadyAsync` is either delivered
or fails with `CommandNotRoutableException` (never a silent timeout; `SendRightAfterRegistration_*` test).

### Key Interfaces

- `IDistributionEventHubService` - Main service for publishing/sending events
- `IDistributedConsumer<TMessage>` - Consumer interface for handling messages
- `ICommandClient<TRequest>` - Typed RPC client for specific commands
- `IRoutedCommandClient<TRequest>` - Dynamic-target RPC client
- `IDistributedCacheService` - File caching with TTL support

### Configuration Pattern

Services configure the event hub via `AddDistributionEventHub()`:

```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "my-service";  // Required
    config.InstancePrefix = "prod";              // Required - isolates environments

    config.AddBroadcastEventConsumer<TConsumer, TMessage>();
    config.AddRoutedEventConsumer<TConsumer, TMessage>("queue-name");
    config.AddCommandConsumer<TConsumer, TMessage>("command-name");
    config.AddCommandClient<TRequest>("target-service");
});
```

### Instance Prefix

All queue/exchange names are prefixed with `InstancePrefix` (default: "default") to isolate environments sharing the same RabbitMQ cluster. Can be set via:
- `DistributionEventHubOptions.InstancePrefix` in appsettings.json
- `config.InstancePrefix` in code (takes precedence)

**Hangfire scheduler queue (AB#5867).** MassTransit's `HangfireEndpointDefinition` ignores the endpoint
name formatter, so the scheduler queue was the un-prefixed `hangfire` in every instance while the
scheduling exchanges bound into it are prefixed. Two instances on one broker (test-2 `main` + `dev`)
consumed it as competing consumers and stored each other's recurring jobs in the wrong Hangfire database,
where `RemoveRecurringJobsByScheduleGroup` never reaches them (doubled cron ticks, jobs surviving
`UndeployTriggers`). `AddHangfireMessageScheduler` now sets the queue to `{prefix}-hangfire`, and
`LegacyHangfireSchedulerBindingRemover` (bus observer, `PreStart`) unbinds this instance's
`{prefix}-MassTransit.Scheduling:*` exchanges from the legacy `hangfire` exchange — other instances'
bindings stay, so a not yet updated instance keeps working. Jobs that already landed in a foreign
Hangfire database are not moved; remove them there by hand. Tests:
`UnitTests/Configuration/HangfireSchedulerTopologyTests`, `IntegrationTests/Messaging/HangfireSchedulerIsolationTests`.

### Command consumers: exclusive (classic) versus shared (AB#6909)

`AddCommandConsumer(name)` and `RegisterCommandConsumer(name, handler)` (without options) declare a non-durable
auto-delete queue, which MassTransit forces to **exclusive**: one process at a time consumes the command, further
replicas wait. This is **deprecated** (a warning is logged once per command name; no `[Obsolete]`, because every consumer
repository builds with warnings as errors). The shared topology consumes the same command with any number of replicas.

| | Classic (exclusive) | Shared, plain (CK methods) | Shared, migrating (`MigratesLegacyCommand = true`) |
|---|---|---|---|
| Register | `AddCommandConsumer(name)` / `RegisterCommandConsumer(name, h)` | `RegisterCommandConsumer(name, h, new CommandConsumerOptions())` | `AddCommandConsumer(name, new CommandConsumerOptions { MigratesLegacyCommand = true })` or `RegisterCommandConsumer(name, h, same options)` |
| Queue | `{p}-{name}` exclusive, non-durable, auto-delete | `{p}-{name}` durable, auto-delete, non-exclusive | `{p}-{name}::shared` durable, auto-delete, non-exclusive |
| Sender | `AddCommandClient` / `GetCommandResponseAsync(..., timeout)` -> exchange `{p}-{name}` | `GetCommandResponseAsync(..., CommandSendOptions)` -> exchange `{p}-{name}::send` | **both**: classic senders (unchanged) via the bridge, `CommandSendOptions` senders via `::send` |

**Why a distinct queue name.** RabbitMQ refuses to redeclare a queue or exchange with other arguments (PRECONDITION_FAILED,
406) and an exclusive queue is closed to every other connection (RESOURCE_LOCKED, 405). Reusing `{p}-{name}` makes an old and
a new replica fail on each other (pinned by `Premise_SameNameSharedTopology_ConflictsWithTheClassicQueueAndExchange`). The
plain shared topology is therefore only for NEW names (CK methods); a command that has an exclusive history uses the
migrating variant.

**The bridge (`LegacyCommandBridge`).** Senders of the ~15 repositories that call `AddCommandClient` must not change. A
migrated consumer therefore also serves the classic sender address: its shared queue is bound to the classic exchange
`{p}-{name}` **only while no classic consumer exists**. A fanout exchange copies a message into every bound queue, so a
permanent binding would process each classic command twice while an old replica runs. Every replica of the migrated consumer
runs the same idempotent reconcile every `LegacyProbeInterval` (default 2 s): passive declare of the classic queue (405 or
success = alive, 404 = gone) -> alive: unbind (new replicas stand by, the old one keeps all classic traffic); gone: declare the
exchange with the classic arguments (fanout, non-durable, auto-delete) and bind. The shared queue is never created by the
bridge. Known windows, each bounded by the interval: after the last old replica stopped, classic commands reach no queue until
the next pass (the sender's timeout/retry covers it, as for every restart of the former single consumer); if a classic replica
(re)starts while the shared queue is bound (rollback), commands are copied to both until the next pass.

**Rolling update (old = exclusive, new = migrating), one service at a time:**
1. Release the event hub library (this change). No behaviour changes until a consumer opts in.
2. Switch the consumer registrations of ONE service to `MigratesLegacyCommand = true` (AB#6910 for bot-services and identity).
   Senders are untouched, so there is no order between services and no sender-first or consumer-first constraint.
3. Roll the service. While an old replica is alive it keeps every classic command; the new replicas start without error and
   stand by (INFO "a classic exclusive consumer ... is alive"). When the last old replica terminates, the new replicas bind
   and consume (INFO "the shared queue now also serves classic senders"), and from then on all replicas share the work.
4. Commands that were ready in the old (non-durable) queue when its last replica stops are lost with that queue, exactly as
   at every restart of the former single consumer; commands already delivered to the old replica are completed before it
   stops. Keep the termination grace period of the old pods and scale one replica at a time to keep the window small.
5. Rollback: stop the migrated replicas first, then start the old version (otherwise commands are processed twice until the
   next bridge pass).
6. After every service of the fleet runs the migrated version, senders MAY move to `CommandSendOptions` (mandatory send,
   `CommandNotRoutableException`); the classic address stays served by the bridge until the last classic sender is gone.

The handler must be safe under competing consumers (no state shared between messages, no "only one instance" assumption).
`AddCommandConsumer(name, options)` does not support `SuppressFaults` (use the dynamic registration for sensitive payloads).
Operations: queues named `*::shared` and exchanges `*::send` are new names; broker policies or monitoring that match command
queue names (Platform Ops) must include them. Details and API: `docs/DEVELOPER.md` -> "Migrating exclusive command queues".

**Command consumers in the monorepo** (snapshot of `main` on 2026-10-18; senders in the last column):

| Repository | Registration | Command (queue name) | Kind | Senders |
|---|---|---|---|---|
| octo-bot-services (`Program.cs`) | `AddCommandConsumer` (`ModelCommandsConsumer`) | `octo::bot::import-ck`, `import-ck-batch`, `import-rt`, `export-rt-by-query`, `export-rt-by-deep-graph` | exclusive, migration candidate | octo-asset-repo-services (`ModelsController`, `AddCommandClient`) |
| octo-bot-services (`Program.cs`) | `AddCommandConsumer` (`RecurringJobConsumer`) | `octo::bot::remove-recurring-jobs-by-schedule-group` | exclusive, migration candidate | octo-communication-controller-services, octo-ai-services, octo-mcp-service (`AddCommandClient`) |
| octo-identity-services (`Program.cs`) | `AddCommandConsumer` (`CreateIdentityDataCommandRequestConsumer`) | `octo::identity::create-identity-data` | exclusive, migration candidate | asset-repo, bot, communication-controller, ai, mcp, report (`AddCommandClient`, `DefaultConfigurationCreatorService`) |
| octo-communication-sdk (`FromPipelineDataEventNode`) | `RegisterCommandConsumer` per pipeline | `pipelinedatacommand-{tenant}-dataflow-{id}-pipeline-{id}` | exclusive, addressed to one pipeline instance | `ToPipelineDataEventNode` (`GetCommandResponseAsync`) |
| octo-communication-sdk (`FromExecutePipelineCommandNode`) | `RegisterCommandConsumer` per pipeline | `octo::...execute-pipeline-{tenant}-pipeline-{id}` (`PipelineQueueNames.ExecutePipelineCommand`) | exclusive, addressed to one pipeline instance | communication-controller `TriggerManagementService` (`IRoutedCommandClient`) |
| octo-mesh-adapter (`FromSendNotificationNode`) | `RegisterCommandConsumer` per data flow | `octo::com-controller::send-notification-pipeline-{tenant}-data-flow-{id}` | exclusive, addressed to one data flow | communication-controller |
| octo-common-services (`CkMethodHandlerStartupService`), octo-communication-controller-services (`PipelineMethodHost`) | `RegisterCommandConsumer(..., CommandConsumerOptions)` | CK method queues `octo::method::...` | already shared (plain topology, AB#6891/AB#5671) | CK method dispatch |

### Dependencies

- MassTransit.RabbitMQ 8.5.10
- MassTransit.Hangfire 8.5.10 (optional scheduling)
- MongoDB.Driver 3.5.2 (for MongoDB project)
- Meshmakers.Common.Shared (internal dependency)
