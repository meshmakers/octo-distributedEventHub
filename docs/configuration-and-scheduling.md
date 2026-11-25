---
description: Configuration pattern, instance prefix, Hangfire scheduler queue isolation and recurring-send time zones
applies_to: src/DistributionEventHub/Configuration/**, src/DistributionEventHub/RecurringSchedulingOptions.cs, src/DistributionEventHub/Services/OctoRecurringSchedule.cs, tests/**/HangfireScheduler*.cs
---

Recurring sends (`ScheduleRecurringSendAsync` with `RecurringSchedulingOptions`) evaluate the cron in
`RecurringSchedulingOptions.TimeZoneId` (IANA or Windows id). When it is null or empty, the local zone of the process that
creates the schedule is used (usually UTC in a container). Set it whenever the cron names a time of day for a person (the
notification schedule passes each recipient's zone). Usage: `docs/DEVELOPER.md`; tests: `OctoRecurringScheduleTests`.

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

**Hangfire scheduler queue.** MassTransit's `HangfireEndpointDefinition` ignores the endpoint
name formatter, so the scheduler queue was the un-prefixed `hangfire` in every instance while the
scheduling exchanges bound into it are prefixed. Two instances on one broker (for example a `main` and a `dev` instance)
consumed it as competing consumers and stored each other's recurring jobs in the wrong Hangfire database,
where `RemoveRecurringJobsByScheduleGroup` never reaches them (doubled cron ticks, jobs surviving
`UndeployTriggers`). `AddHangfireMessageScheduler` now sets the queue to `{prefix}-hangfire`, and
`LegacyHangfireSchedulerBindingRemover` (bus observer, `PreStart`) unbinds this instance's
`{prefix}-MassTransit.Scheduling:*` exchanges from the legacy `hangfire` exchange — other instances'
bindings stay, so a not yet updated instance keeps working. Jobs that already landed in a foreign
Hangfire database are not moved; remove them there by hand. Tests:
`UnitTests/Configuration/HangfireSchedulerTopologyTests`, `IntegrationTests/Messaging/HangfireSchedulerIsolationTests`.
