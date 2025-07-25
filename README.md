# DistributionEventHub

A distributed event system for OctoMesh services built on MassTransit and RabbitMQ, providing four core messaging patterns with MongoDB-based file caching.

## Architecture Overview

The DistributionEventHub is built on **MassTransit** with **RabbitMQ** as the transport layer and implements a service-wide event system with four main functions.

## Core Features

### 1. Broadcast Events
**Implementation:** `BroadcastEventConsumerDefinition`
- **Pattern:** Fanout exchange with service-specific queues
- **Queue Schema:** `octo::service::{ServiceName}`
- **Behavior:** Every service receives a copy of the event
- **Use Cases:** Tenant reload, global configuration changes

```csharp
// Configuration
config.AddBroadcastEventConsumer<TenantReloadConsumer, TenantReloadEvent>();

// Publishing
await eventHub.PublishAsync(new TenantReloadEvent());
```

### 2. Routed Events (Fire & Forget)
**Implementation:** `RoutedEventConsumerDefinition`
- **Pattern:** Direct exchange with message type as routing key
- **Queue Schema:** `{MessageType.FullName}` or explicit address
- **Load Balancing:** Automatic round-robin with multiple consumer instances
- **Use Cases:** Asynchronous commands without response

```csharp
// Configuration
config.AddRoutedEventConsumer<DataProcessConsumer, ProcessDataEvent>("data-processor");

// Sending
await eventHub.SendAsync(new Uri("queue:data-processor"), processEvent);
```

### 3. CommandClient (Request/Response)
**Implementation:** `CommandClient<TRequest>` & `RoutedCommandClient<TRequest>`
- **Pattern:** RPC over temporary reply queues
- **Transport:** Exchange-based with correlation IDs
- **Features:** Timeout handling, retry mechanism, exception wrapping
- **Variants:**
  - `CommandClient`: Pre-configured for specific commands
  - `RoutedCommandClient`: Dynamic target addressing

```csharp
// Standard CommandClient
config.AddCommandClient<GetUserCommand>("user-service", TimeSpan.FromSeconds(30));
var response = await commandClient.GetResponse<UserResponse>(getUserCmd);

// Routed CommandClient  
var response = await routedClient.GetResponse<UserResponse>("user-service", getUserCmd);
```

### 4. MongoDB File Caching
**Implementation:** `DistributedCacheService`
- **Storage:** MongoDB GridFS via `IRepositoryClient`
- **Features:** TTL support, content-type aware, tenant isolation
- **Repository Resolution:** Via `ITenantResolver` (repository per tenant)

```csharp
// Upload with TTL
var cacheKey = await cacheService.CreateStreamAsync(
    tenantId, fileStream, "application/pdf", "report.pdf", TimeSpan.FromHours(24));

// Download
var cachedFile = await cacheService.GetCacheStreamByIdAsync(tenantId, cacheKey);
```

## Technical Details

### Exception Handling
- `DistributionTimeoutException`: Request timeouts
- `DistributedOperationFailedException`: General errors
- Automatic exception wrapping for better diagnostics

### Consumer Integration
- `DistributedConsumer<TConsumer, TMessage>`: Wrapper for business logic
- `IDistributedContext`: Access to MassTransit context
- Scoped DI integration for all consumers

### Configuration
- Fluent API via `DistributionEventHubConfiguration`
- Service discovery via `UniqueServiceAddress`
- Optional Hangfire scheduling for time-based messages

### Message Routing
- **Broadcast:** `exchange:{MessageType}` → `octo::service::{ServiceName}`
- **Routed:** `queue:{DestinationAddress}` with round-robin
- **Commands:** `exchange:{CommandName}?temporary=true` for RPC

## Getting Started

### Basic Setup

```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "my-service";
    
    // Optional: Configure instance prefix for multi-instance deployments
    // config.InstancePrefix = "prod"; // Isolates this instance from others
    
    // Add consumers
    config.AddBroadcastEventConsumer<MyBroadcastConsumer, MyBroadcastEvent>();
    config.AddRoutedEventConsumer<MyRoutedConsumer, MyRoutedEvent>("my-queue");
    config.AddCommandConsumer<MyCommandConsumer, MyCommand>("my-command");
    
    // Add clients
    config.AddCommandClient<MyCommand>("target-service");
    config.AddRoutedCommandClient<MyRoutedCommand>();
});
```

### Multi-Instance Configuration

For deployments where multiple OctoMesh instances need to share the same message broker while maintaining complete isolation, configure the `InstancePrefix`. There are two ways to set this:

#### Option 1: Configuration-based (Recommended)

Set the instance prefix in your `appsettings.json`:

```json
{
  "DistributionEventHub": {
    "BrokerHost": "localhost",
    "BrokerPort": 5672,
    "InstancePrefix": "production"
  }
}
```

```csharp
services.Configure<DistributionEventHubOptions>(options =>
    hostContext.Configuration.GetSection("DistributionEventHub").Bind(options));

services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "my-service";
    // InstancePrefix is automatically applied from DistributionEventHubOptions
});
```

#### Option 2: Code-based Configuration

```csharp
services.AddDistributionEventHub(config =>
{
    config.UniqueServiceAddress = "my-service";
    config.InstancePrefix = "production"; // or "dev", "staging", "tenant-a", etc.
    
    // All queue and exchange names will be prefixed with "production:"
    // Example: "octo::service::my-service" becomes "production:octo::service::my-service"
});
```

**Instance Isolation Benefits:**
- **Complete Separation**: Different environments (prod/staging/dev) can use the same RabbitMQ cluster
- **Tenant Isolation**: Multi-tenant deployments with dedicated message spaces
- **Blue/Green Deployments**: Run multiple versions simultaneously without interference
- **Testing Isolation**: Integration tests won't interfere with production traffic

**Priority:** Code-based configuration takes precedence over options-based configuration.

### Usage

```csharp
// Inject services
public class MyService
{
    private readonly IDistributionEventHubService _eventHub;
    private readonly ICommandClient<MyCommand> _commandClient;
    private readonly IDistributedCacheService _cacheService;
    
    // Send broadcast event
    await _eventHub.PublishAsync(new MyBroadcastEvent());
    
    // Send routed event
    await _eventHub.SendAsync(new Uri("queue:target"), new MyRoutedEvent());
    
    // Execute command
    var result = await _commandClient.GetResponse<MyResponse>(new MyCommand());
    
    // Cache file
    var cacheKey = await _cacheService.CreateStreamAsync(
        tenantId, stream, "application/json", "data.json", TimeSpan.FromMinutes(30));
}
```

## Dependencies

- **MassTransit**: Message transport abstraction
- **RabbitMQ**: Message broker
- **MongoDB**: File caching storage
- **Hangfire** (optional): Message scheduling

## Architecture Benefits

The implementation abstracts complex MassTransit configuration and provides type-safe APIs for all event patterns. It cleanly separates fire-and-forget events from request/response patterns while offering a content-type-aware caching system with automatic cleanup mechanisms.
