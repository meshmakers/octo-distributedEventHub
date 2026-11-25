# AGENTS.md

Instructions for coding agents in this repository. Detail lives in `docs/`; read the matching file before changing the code it covers.

## What this repo is

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

### Key Interfaces

- `IDistributionEventHubService` - Main service for publishing/sending events
- `IDistributedConsumer<TMessage>` - Consumer interface for handling messages
- `ICommandClient<TRequest>` - Typed RPC client for specific commands
- `IRoutedCommandClient<TRequest>` - Dynamic-target RPC client
- `IDistributedCacheService` - File caching with TTL support

### Dependencies

- MassTransit.RabbitMQ 8.5.12 (stays on 8.x: MassTransit 9 is commercially licensed)
- MassTransit.Hangfire 8.5.12 (optional scheduling)
- MongoDB.Driver 3.14.1 (for MongoDB project)
- Meshmakers.Common.Shared (internal dependency)

## Read before you change

<!-- >>> generated: routing -->
| When you change | Read first |
|---|---|
| `src/**` | `docs/DEVELOPER.md` |
| `tests/**` | `docs/TEST-CONCEPT.md` |
| `src/DistributionEventHub/Services/Legacy*.cs`, `src/DistributionEventHub/Services/EventHubControl.cs`, `src/DistributionEventHub/Configuration/BrokerConnectionFactory.cs`, `tests/**/Legacy*.cs` | `docs/command-consumers.md` |
| `src/DistributionEventHub/Services/*Command*.cs`, `src/DistributionEventHub/Consumers/SharedCommandConsumerDefinition.cs`, `tests/DistributionEventHub.IntegrationTests/Messaging/Command*Tests.cs`, `tests/DistributionEventHub.IntegrationTests/Messaging/RestrictedAdapterUserTests.cs` | `docs/command-endpoints.md` |
| `src/DistributionEventHub/Configuration/**`, `src/DistributionEventHub/RecurringSchedulingOptions.cs`, `src/DistributionEventHub/Services/OctoRecurringSchedule.cs`, `tests/**/HangfireScheduler*.cs` | `docs/configuration-and-scheduling.md` |
<!-- <<< end generated: routing -->

## Build & test

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

## Before you commit

Build and run the tests as described above.

## Rules

