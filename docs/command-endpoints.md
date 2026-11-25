---
description: Shared command endpoints (CK methods), their opt-in overloads, and the restricted adapter broker user
applies_to: src/**/Services/*Command*.cs, src/**/SharedCommandConsumerDefinition.cs, tests/**/Command*Tests.cs, tests/**/RestrictedAdapterUserTests.cs
---

### Shared Command Endpoints (CK methods)

`IEventHubControl.RegisterCommandConsumer(name, handler, CommandConsumerOptions)` and
`IDistributionEventHubService.GetCommandResponseAsync(name, request, CommandSendOptions, ct)` are a **pair**:
competing consumers on a durable auto-delete, non-exclusive queue, senders publish to the stable exchange
`{prefix}-{name}::send`. `CommandSendOptions.Mandatory` → `CommandNotRoutableException` within milliseconds (tests assert under 500 ms) when no consumer is
bound. Await `EndpointHandle.WaitUntilReadyAsync()` before announcing an endpoint. Gotchas (see `docs/DEVELOPER.md`):
MassTransit forces `exclusive` on non-durable auto-delete queues (so legacy command endpoints are single-consumer),
publisher confirms are on by default in MassTransit 8.5.x (checked with 8.5.10), and the bus-level retry (1s/5s/15s) also re-executes
throwing dynamic handlers (4 executions) — method handlers must never throw.
Review fixes (H5/M5/L7): `ExecuteDeliveredCommandHandler` overload with `CommandDeliveryInfo`
(`Redelivered`, `RetryAttempt`, `IsPossibleDuplicate`) so non-idempotent handlers can refuse redeliveries;
`CommandSendOptions.Persistent = false` (transient messages) and `CommandConsumerOptions.SuppressFaults` (no retry, no
`Fault<T>`, no `_error`/`_skipped` copies) for sensitive payloads; a non-caller `OperationCanceledException` maps to
`DistributedOperationFailedException`.

Opt-in: the shared-endpoint overloads are additive. `AddCommandConsumer`, the plain
`RegisterCommandConsumer(name, handler)` and the plain `GetCommandResponseAsync` keep the exclusive, non-durable
auto-delete queue and the `?temporary=true` request exchange (pinned by
`IntegrationTests/Messaging/LegacyCommandEndpointUnchangedTests`). The new interface members have default
implementations that throw `NotSupportedException`, so existing test doubles of `IEventHubControl` /
`IDistributionEventHubService` keep compiling. Migrating the existing exclusive command queues to shared endpoints
is a separate story. A mandatory send right after `RegisterCommandConsumer` without `WaitUntilReadyAsync` is either delivered
or fails with `CommandNotRoutableException` (never a silent timeout; `SendRightAfterRegistration_*` test); a send that is
not mandatory can time out.

### Restricted adapter broker user

Adapters and edge sites connect with a broker user that is refused on every resource whose name contains `octo::method::` and may not write to the
default exchange (details and the three patterns: `docs/DEVELOPER.md`, "Broker permissions"). `RestrictedAdapterUserTests` proves it
(consume/get/bind/declare/delete/purge/publish refused, the adapter bus works). Names of the hub are `{prefix}-{name}`, so permission
patterns must not be start-anchored on `octo::method::`. `OCTO_DEH_IT_AS_ADAPTER=1` runs the integration suite as the adapter user (filter in `docs/DEVELOPER.md`; the two classes with `octo::method::` names need the core user).
