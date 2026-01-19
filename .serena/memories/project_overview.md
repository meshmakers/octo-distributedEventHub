# Octo.DistributedEventHub - Project Overview

## Purpose
**Octo.DistributedEventHub** is a distributed event system for OctoMesh services built on MassTransit and RabbitMQ. It provides four core messaging patterns with MongoDB-based file caching for managing distributed communication across microservices.

## Tech Stack

### Core Technologies
- **.NET 9.0** (primary target framework)
- **netstandard2.0** (for main library compatibility)
- **MassTransit 8.5.5** - Message transport abstraction with RabbitMQ integration
- **RabbitMQ** - Message broker for event distribution
- **MongoDB.Driver 3.5.0** - GridFS-based file caching
- **Hangfire** (optional) - Message scheduling for delayed/recurring messages

### Internal Dependencies
- **Meshmakers.Common.Shared** (version 4.1.26) - Internal shared library

### Development Tools
- **JetBrains Rider / ReSharper** - IDE support (.DotSettings files present)
- **Git** - Version control
- **GitHub** - Repository hosting (https://github.com/meshmakers/octo-distributedEventHub)

## Architecture Patterns

### Four Core Messaging Patterns

1. **Broadcast Events** (`BroadcastEventConsumerDefinition`)
   - Fanout pattern where every service instance receives the event
   - Queue naming: `{InstancePrefix}:octo::service::{ServiceName}`
   - Use cases: Tenant reloads, global configuration changes

2. **Routed Events** (`RoutedEventConsumerDefinition`)
   - Direct/Topic exchange with round-robin load balancing
   - Queue naming: `{InstancePrefix}:{DestinationAddress}`
   - Use cases: Fire-and-forget asynchronous commands

3. **Request/Response Commands**
   - `CommandClient<TRequest>` - Pre-configured for specific commands
   - `RoutedCommandClient<TRequest>` - Dynamic target addressing
   - RPC pattern with temporary reply queues, timeout handling, retry mechanisms

4. **MongoDB File Caching** (`DistributedCacheService`)
   - GridFS-based storage with TTL support
   - Tenant-isolated repositories via `ITenantResolver`
   - Content-type aware with automatic cleanup

### Instance Prefix Architecture
Every deployment MUST configure an `InstancePrefix` to isolate message broker namespaces, enabling:
- Multiple environments (prod/staging/dev) on same RabbitMQ cluster
- Multi-tenant deployments with dedicated message spaces
- Blue/green deployments without interference

## Project Structure

### Main Source Projects
```
src/
├── DistributionEventHub/               # Core library
│   ├── Configuration/                  # Fluent configuration API and DI setup
│   ├── Consumers/                      # Base consumer classes and definitions
│   ├── Services/                       # Core services (CommandClient, EventHub, etc.)
│   ├── Repository/                     # MongoDB repository abstraction
│   ├── Sagas/                          # MassTransit saga support
│   └── Payloads/                       # Message payload types
└── DistributionEventHub.MongoDB/       # MongoDB GridFS integration
```

### Sample Applications
```
samples/
├── SampleServiceEarth/                 # Example service implementation
├── SampleServiceMars/                  # Example service implementation
├── SampleServiceMoon/                  # Example service implementation
├── SampleCli/                          # CLI tool for testing
└── SampleEvents/                       # Shared message contracts
```

### Configuration Files
- **Directory.Build.props** - MSBuild properties, versioning, NuGet configuration
- **Octo.DistributedEventHub.sln** - Solution file
- **CLAUDE.md** - AI assistant instructions
- **README.md** - Project documentation

## Key Service Interfaces

- **IDistributionEventHubService** - Main event publishing/sending interface
- **ICommandClient&lt;TRequest&gt;** - Typed request/response client
- **IRoutedCommandClient&lt;TRequest&gt;** - Dynamic request/response client
- **IDistributedCacheService** - MongoDB GridFS file caching with TTL
- **IEventHubControl** - Manual bus start/stop control
- **IDistributedConsumer&lt;TMessage&gt;** - Base interface for all message consumers
- **ITenantResolver** - Resolves tenant-specific repository instances

## Exception Types

- **DistributionTimeoutException** - Request timeout failures
- **DistributedOperationFailedException** - General operation errors, missing configuration
- **DistributionException** - Base exception type

## Development Environments

- **macOS (Darwin)** - Primary development platform
- **RabbitMQ** - Must be running locally or accessible via network (default port 5672)
- **MongoDB** - Must be running for file caching features (default port 27017)
