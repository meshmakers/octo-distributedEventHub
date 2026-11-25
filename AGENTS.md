# AGENTS.md

Instructions for coding agents in this repository. Detail lives in `docs/`; read the matching file before changing the code it covers.

## What this repo is

`octo-distributedEventHub` builds the NuGet packages `Meshmakers.Octo.Common.DistributionEventHub` and
`Meshmakers.Octo.Common.DistributionEventHub.MongoDB`: the distributed event system of the OctoMesh services, built on
**MassTransit** with **RabbitMQ** transport. The repository is public.

### Project Structure

- `src/DistributionEventHub` - Core library (NuGet: `Meshmakers.Octo.Common.DistributionEventHub`)
  - Targets `net10.0` only
- `src/DistributionEventHub.MongoDB` - MongoDB GridFS integration for file caching
- `tests/DistributionEventHub.UnitTests`, `tests/DistributionEventHub.IntegrationTests` (needs Docker)
- `samples/` - Example services demonstrating usage patterns

### Core Messaging Patterns

1. **Broadcast Events** - Fanout to all services via `PublishAsync<T>()`. Queue pattern:
   `{prefix}-octo::service::{UniqueServiceAddress}-{instance id}`
2. **Routed Events** - Fire-and-forget to specific queues via `SendAsync<T>(Uri, message)` with round-robin
   load balancing
3. **CommandClient** - Request/response RPC via `ICommandClient<TRequest>` and `IRoutedCommandClient<TRequest>`
4. **Distributed Cache** - MongoDB GridFS file caching via `IDistributedCacheService`

### Key Interfaces

- `IDistributionEventHubService` - Main service for publishing/sending events
- `IDistributedConsumer<TMessage>` - Consumer interface for handling messages
- `ICommandClient<TRequest>` - Typed RPC client for specific commands
- `IRoutedCommandClient<TRequest>` - Dynamic-target RPC client
- `IDistributedCacheService` - File caching with TTL support

### Dependencies

Versions are in the project files (`src/DistributionEventHub/DistributionEventHub.csproj`): `MassTransit.RabbitMQ` and
`MassTransit.Hangfire` (message scheduling) stay on 8.x, because MassTransit 9 is commercially licensed;
`MongoDB.Driver` in the MongoDB project; `Meshmakers.Common.Shared` (version `MeshmakerVersion` in
`Directory.Build.props`).

## Read before you change

<!-- >>> generated: routing -->
| When you change | Read first |
|---|---|
| `src/**` | `docs/DEVELOPER.md` |
| `tests/**` | `docs/TEST-CONCEPT.md` |
| `src/**/Services/Legacy*.cs`, `src/**/EventHubControl.cs`, `src/**/BrokerConnectionFactory.cs`, `tests/**/Legacy*.cs` | `docs/command-consumers.md` |
| `src/**/Services/*Command*.cs`, `src/**/SharedCommandConsumerDefinition.cs`, `tests/**/Command*Tests.cs`, `tests/**/RestrictedAdapterUserTests.cs` | `docs/command-endpoints.md` |
| `src/**/Configuration/**`, `src/**/RecurringSchedulingOptions.cs`, `src/**/OctoRecurringSchedule.cs`, `tests/**/HangfireScheduler*.cs` | `docs/configuration-and-scheduling.md` |
<!-- <<< end generated: routing -->

## Build & test

```bash
# Build with local dependencies (DebugL configuration) - the default for local work
dotnet build Octo.DistributedEventHub.sln -c DebugL

# Unit tests
dotnet test tests/DistributionEventHub.UnitTests -c DebugL

# Integration tests (Testcontainers: start their own RabbitMQ / MongoDB containers, Docker required)
dotnet test tests/DistributionEventHub.IntegrationTests -c DebugL

# Build in Release mode against a published release (version must be passed explicitly)
dotnet build Octo.DistributedEventHub.sln -c Release -p:OctoVersion=3.5.1
```

Versions come only from the pipeline: `Directory.Build.props` has no release fallback, so a Debug/Release build without
`-p:OctoVersion=X.Y.Z` (or `-p:OctoNugetPrivateServer=<feed>` for the main line `0.1.*`) fails fast with MSBuild error
`OCTO0001`. Use `-c DebugL` locally; it restores the Octo packages from `../nuget` next to the repository.

CI (`azure-pipelines.yml`): `Build src and tests` builds `src/**` plus the `*Tests` projects once; `Test` then runs
`dotnet test --no-build --no-restore` per project, sequentially. A new test project outside `tests/**/*Tests.csproj`
must be added to the build step's project list. Set `OCTO_DEH_TEST_LOG=/path/file.log` to get the timing diagnostics of
`CommandSendOptionsTests` (mandatory-return latency, round-trip p50/p95) written to a file.

## Before you commit

1. `dotnet build Octo.DistributedEventHub.sln -c DebugL` succeeds: warnings are errors (`Directory.Build.props`).
2. `dotnet test tests/DistributionEventHub.UnitTests -c DebugL` is green; also the integration project when you changed
   messaging, topology or caching code, and once with `OCTO_DEH_IT_AS_ADAPTER=1` for a new endpoint, binding or publish
   (rule 4).
3. Changed usage of the library: update `docs/DEVELOPER.md`; `README.md` is the packed NuGet readme
   (`Directory.Build.props`). Commits that touch only `*.md` files or `docs/` do not start CI (`azure-pipelines.yml`).
4. Touched `AGENTS.md` or a file in `docs/`: `Test-OctoAgentDocs -Fix` (octo-tools) reports 0 errors. It rewrites the
   generated routing table, so run it before you stage.

## Rules

- **This repository is public.** No work item ids, internal hostnames, environment, customer or tenant names, or
  secrets in docs and in new or changed code comments and test names; remove an existing one when you touch its line
  (77 lines with an `AB#` id remain under `src/`, `tests/` and `samples/`).
- **A queue or exchange that exists on a broker keeps its name and arguments; new behaviour gets a new name.**
  RabbitMQ refuses to redeclare a queue or exchange with other arguments (PRECONDITION_FAILED) and closes an exclusive
  queue to every other connection (RESOURCE_LOCKED), so during a rolling update an old and a new replica fail on each
  other. Shared endpoints therefore use `{prefix}-{name}::shared` and `{prefix}-{name}::send`; the classic topology of
  `AddCommandConsumer(name)` / `RegisterCommandConsumer(name, handler)` is pinned by
  `tests/DistributionEventHub.IntegrationTests/Messaging/LegacyCommandEndpointUnchangedTests.cs:20`, the conflict by
  `LegacyCommandMigrationTests.cs:386`. Exception: the scheduler queue became `{prefix}-hangfire`, and a bus observer
  unbinds the old exchanges (`LegacyHangfireSchedulerBindingRemover`,
  `src/DistributionEventHub/Configuration/HangfireSchedulerTopology.cs:62`). Detail: `docs/command-consumers.md`.
- **Every queue, exchange and scheduler name goes through the instance prefix.** Instances (environments) share one
  RabbitMQ cluster; `CacheCommon.ApplyInstancePrefix` and `ApplyInstancePrefixToUri`
  (`src/DistributionEventHub/CacheCommon.cs:60`, `:71`) apply it to explicit names, and the exchanges of message types
  and the auto-configured endpoints get it from `PrefixEntityNameFormatter` and `DefaultEndpointNameFormatter`, both
  built with `CacheCommon.GetInstancePrefix` (`CacheCommon.cs:25`;
  `src/DistributionEventHub/Configuration/DependencyInjection/ServiceCollectionExtensions.cs:139-144`). The scheduler
  queue once bypassed it, and two instances stored each other's recurring jobs (`HangfireSchedulerTopology.cs:17`).
  Pinned by `InstanceIsolationTests` and `HangfireSchedulerIsolationTests` (integration project, `Messaging/`).
  Detail: `docs/configuration-and-scheduling.md`.
- **A new endpoint, binding or publish must work for the restricted adapter user.** Adapters and edge sites connect
  with a broker user that has no configure, write or read right on a name containing `octo::method::` and no write on
  the default exchange (`tests/DistributionEventHub.IntegrationTests/Fixtures/RestrictedAdapterUser.cs:20-22`); the
  default test user is `guest`, so a new test passes without noticing. Run the integration suite with
  `OCTO_DEH_IT_AS_ADAPTER=1` (`Fixtures/RabbitMqFixture.cs:31`): every test that uses no `octo::method::` name must
  pass. Names are `{prefix}-{name}`, so such a pattern must not be start-anchored. Exception: the CK method endpoints
  use `octo::method::` names and need the core user (`Messaging/RestrictedAdapterUserTests.cs:22`). Detail:
  `docs/command-endpoints.md`.
- **Open RabbitMQ connections only through `BrokerConnectionFactory.Create`**
  (`src/DistributionEventHub/Configuration/BrokerConnectionFactory.cs:27`, the only `new ConnectionFactory` in
  `src/`). It applies `BrokerUser` and `BrokerPassword` the way MassTransit does (null or whitespace: not applied, the
  RabbitMQ client default stays); assigning them directly fails while the bus connects when no broker user is
  configured (unit tests `tests/DistributionEventHub.UnitTests/Configuration/BrokerConnectionFactoryTests.cs:15`;
  integration test `NoBrokerUserConfigured_ConnectsWithTheMassTransitDefaults_AndBinds`,
  `tests/DistributionEventHub.IntegrationTests/Messaging/LegacyCommandBridgeTests.cs:106`). Exception: seven
  integration tests open a raw `new ConnectionFactory` on purpose, to inspect the broker topology or connect as a
  given user (for example `tests/DistributionEventHub.IntegrationTests/Messaging/CommandDeliveryTests.cs:69`,
  `RestrictedAdapterUserTests.cs:83`).
