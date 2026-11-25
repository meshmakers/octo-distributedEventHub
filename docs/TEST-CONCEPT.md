---
description: "Test concept: the unit and integration test projects, their fixtures, categories, the adapter-user run and timing assertions under parallel load"
applies_to: tests/**
---
# Test Concept - DistributionEventHub

The library has two test projects under `tests/`, both `net10.0`, xUnit v3 (`xunit.v3.mtp-off`) with FluentAssertions.
Read the tests themselves for the behaviour they pin; this file only says how the suite is organised and run.

| Project | Needs | Mocks and fixtures | Trait |
|---|---|---|---|
| `tests/DistributionEventHub.UnitTests` | nothing | Moq for the bus, the download and tenant seams | `Category=Unit` |
| `tests/DistributionEventHub.IntegrationTests` | Docker (Testcontainers) | a real RabbitMQ and MongoDB | `Category=Integration`, plus `RoutedEventExplicitQueue` and `RoutedEventLatestOnly` on two classes |

## Unit tests

Folders follow the namespaces of `src/DistributionEventHub`: `Configuration` (options, topology, broker connection
factory), `Consumers`, `Exceptions`, `Services` (command client, cache service, event hub service, the legacy command
bridge probes and schedule, recurring schedules, send options) and `Utilities` (`CacheCommon`). They run in seconds and
must stay free of a broker. Run them for every change:

```bash
dotnet test tests/DistributionEventHub.UnitTests -c DebugL
```

## Integration tests

They start their own containers, so Docker must run (`rabbitmq:3-management`, `mongo:7`); nothing is shared with a
developer's local broker.

- `Fixtures/RabbitMqFixture.cs` starts one RabbitMQ per test collection `RabbitMQ`, with the user `guest`, and creates the
  restricted adapter user of `Fixtures/RestrictedAdapterUser.cs`. `Fixtures/MongoDbFixture.cs` does the same for the
  collection `MongoDB` (`Caching/MongoDbCachingTests.cs`). `Fixtures/TestHelpers.cs` holds the shared helpers.
- `Messaging/` covers broadcast, routed and topic events, the command client and its delivery and send options, the
  legacy command bridge and the classic-to-shared queue migration, instance isolation (two instances on one broker, also
  for the scheduler queue) and the restricted adapter user.
- With `OCTO_DEH_IT_AS_ADAPTER=1` the whole suite connects as the restricted adapter user instead of `guest`; every test
  that uses no `octo::method::` name must still pass. Run it once for a new endpoint, binding or publish
  (`AGENTS.md`, `docs/command-endpoints.md`).
- `OCTO_DEH_TEST_LOG=/path/file.log` writes the timing diagnostics of `CommandSendOptionsTests` to a file.

```bash
dotnet test tests/DistributionEventHub.IntegrationTests -c DebugL
```

Tests that must inspect or provoke the broker open a raw RabbitMQ connection on purpose (`AGENTS.md`, rule on
`BrokerConnectionFactory`).

## CI

`azure-pipelines.yml` builds `src/**` and every `tests/**/*Tests.csproj` once, then runs `dotnet test --no-build
--no-restore` per project, sequentially (projects named `*SystemTests.csproj` are excluded). A test project outside that
pattern must be added to the build step's project list.

## Timing assertions under parallel load

A wall-clock bound measures the component **and** the test host. Under the full parallel suite the host delivers timer
ticks and `Task.Delay` continuations late and a broker round trip can take seconds, so a tight wall-clock bound fails
without a defect. Rules:

- Prove a time bound of a component on the component's own clock: inject a `TimeProvider` and drive it with
  `FakeTimeProvider` (`Microsoft.Extensions.TimeProvider.Testing`) one tick at a time, with a real-time deadline only as a
  hang guard. The example is `BackgroundLoop_ClassicConsumerAppearsAfterFullBackOff_…` in
  `tests/DistributionEventHub.IntegrationTests/Messaging/LegacyCommandBridgeTests.cs:356`. Do not widen the slack of a
  wall-clock bound to make it pass.
- Keep wall-clock bounds only where the slack is a large multiple of the expected time (for example
  `BackgroundLoop_SharedConsumerCountChanges_…`, same file: under 3 s for a reaction of about one 100 ms tick).
- A flaky timing test is a bug with a root cause (trace the loop, measure, then fix); never "pre-existing".
