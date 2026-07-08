# ADR-0001: Layering

## Status

Accepted

## Date

2026-07-08

## Context

EventMesh must support seven message brokers (plus InMemory for testing) behind a single application-facing API. The framework will be consumed as NuGet packages by enterprise applications that may only need one broker, and the contracts package must remain stable across major versions with minimal breaking changes.

We need a layering model that:

- Keeps the public API surface small and broker-independent
- Allows transport packages to be added or removed without affecting unrelated code
- Prevents circular dependencies between runtime, transports, and tooling
- Enables the abstractions package to have zero external dependencies

## Decision

EventMesh uses a four-layer package architecture:

### Layer 0 — Contracts (`EventMesh.Abstractions`)

Zero-dependency package containing all public interfaces, enums, records, and exceptions:

- `IMessageBus`, `IMessageHandler`, `IMessageConsumer`
- `IBrokerTransport`, `IBrokerTransportFactory`, `BrokerCapabilities`
- CloudEvents envelope types, filter contracts, options records
- Exception hierarchy including `CapabilityNotSupportedException`

Applications reference this package for handler interfaces and message types. It never references implementation packages.

### Layer 1 — Runtime (`EventMesh.Core` and supporting packages)

Implementation of the messaging engine:

- `EventMesh.Core` — pipeline, capability engine, serialization, retry, scheduling, DI extensions
- `EventMesh.Storage.PostgreSql` — outbox, inbox, scheduler
- `EventMesh.Storage.Redis` — optional cache and distributed locks
- `EventMesh.SchemaRegistry` — event schema versioning
- `EventMesh.Security` — encryption and authentication plugins

These packages depend only on `EventMesh.Abstractions` and approved infrastructure libraries (Npgsql, Dapper, etc.).

### Layer 2 — Transports (`EventMesh.Transport.*`)

One NuGet package per broker:

- `EventMesh.Transport.RabbitMQ`
- `EventMesh.Transport.Kafka`
- `EventMesh.Transport.RedisStreams`
- `EventMesh.Transport.AzureServiceBus`
- `EventMesh.Transport.AmazonSqs`
- `EventMesh.Transport.GooglePubSub`
- `EventMesh.Transport.Nats`
- `EventMesh.Transport.InMemory`

Each transport depends on `Abstractions` and `Core`. Transports never depend on each other.

### Layer 3 — Tooling

Optional operational packages:

- `EventMesh.Management.Api` — control plane REST API and SignalR
- `EventMesh.Cli` — command-line tool
- `EventMesh.Plugin.Sdk` — plugin authoring SDK

Tooling depends on `Core` but `Core` must not depend on tooling.

## Consequences

### Positive

- Applications depend only on the packages they need (e.g., `Core` + `Transport.Kafka`)
- The abstractions package can be versioned independently with strict compatibility guarantees
- New brokers are added by creating a new transport package without modifying existing code
- Clear dependency direction prevents architectural erosion

### Negative

- Some types must be duplicated or mapped between layers (e.g., internal transport messages vs public envelope)
- Cross-cutting changes may require updates across multiple packages
- Developers must understand which package to reference for each concern

### Neutral

- `Directory.Build.props` enforces consistent `net10.0` target, nullable, and warnings-as-errors across all packages

## References

- [ARCHITECTURE.md](../../ARCHITECTURE.md)
- [ADR-0008: Control Plane vs Data Plane](0008-control-plane-vs-data-plane.md)
