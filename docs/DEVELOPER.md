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
counted as waiting by the broker.

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

Example with `InstancePrefix = "prod"`:
- Broadcast: `prod:octo::service::UserService-abc123`
- Routed: `prod:data-processor`
- Command: `prod:get-user`
