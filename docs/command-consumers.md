---
description: Exclusive versus shared command consumers: topologies, the legacy bridge and its probe, rolling update, consumers in the monorepo
applies_to: src/DistributionEventHub/Services/Legacy*.cs, src/DistributionEventHub/Services/EventHubControl.cs, src/DistributionEventHub/Configuration/BrokerConnectionFactory.cs, tests/**/Legacy*.cs
---

### Command consumers: exclusive (classic) versus shared

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
bridge. **Probe back-off:** a passive declare of a missing queue is a channel exception that RabbitMQ logs as an
error, and "no classic queue" is the steady state after the migration (~600k error lines per cluster and day at a fixed 2 s
with 3.5.65). While `Bound`, the classic probe backs off 1, 2, 4, … intervals up to 60 intervals (2 min at the default;
`LegacyProbeSchedule`, `MaxIntervalFactor`); every other state probes every interval. Between backed-off probes the bridge
reads the consumer count of the shared queue on one reused channel (no channel exception); a changed count (rollout/rollback
replaces replicas) probes at once and restarts the back-off. Known windows: after the last old replica stopped, classic
commands reach no queue until the next pass (≤ one interval; the sender's timeout/retry covers it, as for every restart of the
former single consumer); if a classic replica (re)starts while the shared queue is bound (rollback), commands are copied to
both until the next classic probe — ≤ 61 intervals (2 min 2 s), ≤ one interval when the shared consumer count changes too.
**No RabbitMQ errors in the steady state.** The back-off bounded the errors per bridge, but one bridge runs per
migrated command *and replica* (a test environment with 2 replicas: 0.233 ERROR lines/s, alarm `ApplicationErrorLogRateHigh` at 0.2/s).
Decision: while `Bound`, the classic probe is a **mandatory publish** to the default exchange (routing key = classic queue
name, publisher confirms, `LegacyClassicProbe`): no queue → `basic.return`, which RabbitMQ does not log; queue present → the
probe is routed, the bridge steps aside. Every other state (first pass, `LegacyConsumerPresent`, `Faulted`) keeps the passive
declare, so a live classic consumer during a migration rollout never receives probes; while the shared queue is missing the
classic queue is not probed at all. A routed probe reaches a classic consumer only in the rollback case, once per detection:
transient, `expiration: 0`, a MassTransit envelope of an unhandled type (`urn:message:…:LegacyCommandBridgeProbe`), so a
classic MassTransit endpoint skips it instead of faulting. A broker user without write on `amq.default` (the restricted
adapter user) gets 403 once; the bridge then falls back to the passive declare with the back-off for its lifetime
(WARN log). Net effect: at most one `not_found` error pair per bridge start, none afterwards, whatever the replica count; the
back-off, the consumer-count trigger and the reaction bound are unchanged. Rejected: the management HTTP API (extra
endpoint, port and user tag per environment), "stop probing until restart" (a rollback would double-process until the next
restart) and a per-service off switch (needs a config change per environment and a decision when the migration is done;
can still be added later). Tests: `LegacyProbeScheduleTests`, `LegacyClassicProbeTests` (unit),
`BackgroundLoop_NoClassicQueue_TwoReplicas_NoRecurringChannelExceptions` (counts RabbitMQ's `not_found` log lines in the
testcontainer), `Bound_ClassicConsumerAppears_PublishProbeSeesIt_…`, `SharedQueueMissing_DoesNotProbeTheClassicQueue`,
`UserWithoutDefaultExchangeWrite_FallsBackToPassiveDeclare_…`, `BackgroundLoop_ClassicConsumerAppearsAfterFullBackOff_…`,
`BackgroundLoop_SharedConsumerCountChanges_…`.
**Clock:** the loop timer and the schedule's clock come from an optional `TimeProvider` constructor argument of
`LegacyCommandBridge` (`TimeProvider.System` by default, production timing unchanged). The reaction-bound test drives a
`FakeTimeProvider` tick by tick (`CompletedPasses` is the sync point), because the wall-clock version failed under the full
parallel suite from host scheduling delays, not from the bridge (`docs/TEST-CONCEPT.md`, "Timing assertions under parallel load").
**Broker credentials:** the bridge and the Hangfire legacy binding cleanup build their RabbitMQ connection with
`Configuration/BrokerConnectionFactory`, which applies `BrokerUser`/`BrokerPassword` exactly like MassTransit 8.5.10 (null or
whitespace → not applied, RabbitMQ client default `guest`). Never assign `RabbitMqTransportOptions.User`/`Pass` to a
`ConnectionFactory` directly: with no broker user configured that throws `Value cannot be null (Parameter 'userName')` while
the bus connects. Tests: `BrokerConnectionFactoryTests`, `NoBrokerUserConfigured_ConnectsWithTheMassTransitDefaults_AndBinds`.
Every migrated command has its own bridge. For DI registrations one `LegacyCommandBridgeObserver` per configuration owns
the bridges of all migrated commands, because MassTransit's `AddBusObserver` keeps only one registration per observer type
(`TryAddEnumerable`); one observer per command served only the first migrated command of a process (fixed in
the release after 3.5.64; pinned by `SeveralMigratedCommandsInOneProcess_…` and
`AddCommandConsumer_SeveralMigratedCommands_OneObserverStartsOneBridgePerCommand`). Do not register further per-command
bus observers with `AddBusObserver`. Dynamic registrations (`RegisterCommandConsumer`) create a bridge per handle.

**Rolling update (old = exclusive, new = migrating), one service at a time:**
1. Release the event hub library (this change). No behaviour changes until a consumer opts in.
2. Switch the consumer registrations of ONE service to `MigratesLegacyCommand = true` (bot-services and identity).
   Senders are untouched, so there is no order between services and no sender-first or consumer-first constraint.
3. Roll the service. While an old replica is alive it keeps every classic command; the new replicas start without error and
   stand by (INFO "a classic exclusive consumer ... is alive"). When the last old replica terminates, the new replicas bind
   and consume (INFO "the shared queue now also serves classic senders"), and from then on all replicas share the work.
4. Commands that were ready in the old (non-durable) queue when its last replica stops are lost with that queue, exactly as
   at every restart of the former single consumer; commands already delivered to the old replica are completed before it
   stops. Keep the termination grace period of the old pods and scale one replica at a time to keep the window small.
5. Rollback: stop the migrated replicas first, then start the old version (otherwise commands are processed twice until the
   next classic probe of the bridge, see the back-off above).
6. After every service of the fleet runs the migrated version, senders MAY move to `CommandSendOptions` (mandatory send,
   `CommandNotRoutableException`); the classic address stays served by the bridge until the last classic sender is gone.

The handler must be safe under competing consumers (no state shared between messages, no "only one instance" assumption).
`AddCommandConsumer(name, options)` does not support `SuppressFaults` (use the dynamic registration for sensitive payloads).
Operations: queues named `*::shared` and exchanges `*::send` are new names; broker policies or monitoring that match command
queue names (Platform Ops) must include them. Details and API: `docs/DEVELOPER.md` -> "Migrating exclusive command queues".

**Exclusive by design.** A consumer that must stay exclusive (exactly one instance owns the address) declares it
with `CommandConsumerOptions.ExclusiveByDesign("<reason>")` on `AddCommandConsumer(name, options)` or
`RegisterCommandConsumer(name, handler, options)`: classic exclusive topology unchanged, no deprecation warning. An empty
reason, or a combination with `MigratesLegacyCommand`, `LegacyProbeInterval` or `ConfigureConsumeTopology`, is rejected at
registration. Put the reason into the "Exclusive-by-design reason" column below; "—" means not declared yet (still warns).
Details: `docs/DEVELOPER.md` -> "Exclusive by design".

**Command consumers in the monorepo** (snapshot of `main` on 2026-11-06; senders in the fifth column). The two
octo-bot-services rows were migrated with meshmakers/octo-bot-services#51 (merged 2026-11-06); the
octo-mesh-adapter row declares its consumer exclusive by design.

| Repository | Registration | Command (queue name) | Kind | Senders | Exclusive-by-design reason |
|---|---|---|---|---|---|
| octo-bot-services (`Consumers/BotCommandConsumers.cs`, called from `Program.cs`) — octo-bot-services#51 | `AddCommandConsumer(name, CommandConsumerOptions { MigratesLegacyCommand = true })` (`ModelCommandsConsumer`) | `octo::bot::import-ck`, `import-ck-batch`, `import-rt`, `export-rt-by-query`, `export-rt-by-deep-graph` (queues `…::shared`) | shared, migrated | octo-asset-repo-services (`ModelsController`, `AddCommandClient`) | n/a (shared) |
| octo-bot-services (`Consumers/BotCommandConsumers.cs`) — octo-bot-services#51 | `AddCommandConsumer(name, CommandConsumerOptions { MigratesLegacyCommand = true })` (`RecurringJobConsumer`) | `octo::bot::remove-recurring-jobs-by-schedule-group` (queue `…::shared`) | shared, migrated | octo-communication-controller-services, octo-ai-services, octo-mcp-service (`AddCommandClient`) | n/a (shared) |
| octo-identity-services (`Consumers/IdentityCommandConsumers.cs`, called from `Program.cs`) | `AddCommandConsumer(name, CommandConsumerOptions { MigratesLegacyCommand = true })` (`CreateIdentityDataCommandRequestConsumer`) | `octo::identity::create-identity-data` (queue `…::shared`) | shared, migrated | asset-repo, bot, communication-controller, ai, mcp, report (`AddCommandClient`, `DefaultConfigurationCreatorService`) | n/a (shared) |
| octo-communication-sdk (`FromPipelineDataEventNode`) — octo-communication-sdk#38 | `RegisterCommandConsumer(name, h, CommandConsumerOptions.ExclusiveByDesign(…))` per pipeline | `pipelinedatacommand-{tenant}-dataflow-{id}-pipeline-{id}` | exclusive by design, addressed to one pipeline instance | `ToPipelineDataEventNode` (`GetCommandResponseAsync`) | Per-pipeline address, classic sender; shared would need one legacy bridge per pipeline (2 s probe) for every pipeline of every adapter, so it moves only with a sender migration (`FromPipelineDataEventNode.CommandExclusiveByDesignReason`) |
| octo-communication-sdk (`FromExecutePipelineCommandNode`) — octo-communication-sdk#38 | `RegisterCommandConsumer(name, h, CommandConsumerOptions.ExclusiveByDesign(…))` per pipeline | `octo::...execute-pipeline-{tenant}-pipeline-{id}` (`PipelineQueueNames.ExecutePipelineCommand`) | exclusive by design, addressed to one pipeline instance | communication-controller `TriggerManagementService` (`IRoutedCommandClient`) | Per-pipeline address, classic sender; a further adapter replica is a hot standby; shared would need one legacy bridge per pipeline (2 s probe), so it moves only with a sender migration (`FromExecutePipelineCommandNode.ExclusiveByDesignReason`) |
| octo-mesh-adapter (`FromSendNotificationNode`) | `RegisterCommandConsumer(name, h, CommandConsumerOptions.ExclusiveByDesign(…))` per data flow | `octo::com-controller::send-notification-pipeline-{tenant}-data-flow-{id}` | exclusive by design, one endpoint per data flow | none by name: identity-services (`NotificationEmailOtpDeliveryChannel`) and common-services (`NotificationService`) **publish** `SendNotificationsRequest` (`PublishAsync`), which reaches the queue through its message-type binding | The endpoint is fed by publish through the classic queue's default consume topology; exclusive by design keeps that binding, the shared options switch it off (and exclusive by design refuses `ConfigureConsumeTopology`), so it moves only with a change of the publishers; a further adapter replica is a hot standby (`FromSendNotificationNode.ExclusiveByDesignReason`) |
| octo-common-services (`CkMethodHandlerStartupService`), octo-communication-controller-services (`PipelineMethodHost`) | `RegisterCommandConsumer(..., CommandConsumerOptions)` | CK method queues `octo::method::...` | already shared (plain topology) | CK method dispatch | n/a (shared) |
