# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build the entire solution
dotnet build Octo.DistributedEventHub.sln

# Build in Release mode
dotnet build Octo.DistributedEventHub.sln -c Release

# Build with local dependencies (DebugL configuration)
dotnet build Octo.DistributedEventHub.sln -c DebugL
```

There are no unit tests in this repository.

## Architecture Overview

This is a distributed event system for OctoMesh services built on **MassTransit** with **RabbitMQ** transport. The library provides type-safe APIs for four core messaging patterns.

### Project Structure

- `src/DistributionEventHub` - Core library (NuGet: `Meshmakers.Octo.Common.DistributionEventHub`)
  - Multi-targets: `net10.0` and `netstandard2.0`
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

### Dependencies

- MassTransit.RabbitMQ 8.5.7
- MassTransit.Hangfire 8.5.7 (optional scheduling)
- MongoDB.Driver 3.5.2 (for MongoDB project)
- Meshmakers.Common.Shared (internal dependency)
