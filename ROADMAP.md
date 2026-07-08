# EventMesh Roadmap

This document tracks delivery milestones for EventMesh. Each milestone ends with a compilable, tested, and documented repository state.

**Current version:** 0.1.0  
**Target framework:** .NET 10 (`net10.0`)

## Status Legend

| Status | Meaning |
|--------|---------|
| In Progress | Active development; partial deliverables may land on `main` |
| Pending | Not yet started; dependencies from prior milestones must complete first |
| Complete | Delivered, tested, and documented |

## Milestones

### M1 — Architecture + Scaffolding

**Status:** In Progress

Establish the repository foundation: solution structure, root documentation, Architecture Decision Records (ADRs 1–8), broker capability matrix, central package management, CI pipeline, and Docker Compose for local broker development.

**Deliverables:**
- Solution with project skeletons for all core, transport, storage, and tooling packages
- `Directory.Build.props` and `Directory.Packages.props`
- Root docs: README, ARCHITECTURE, ROADMAP, CONTRIBUTING, SECURITY, CHANGELOG, LICENSE, CODE_OF_CONDUCT, SUPPORT
- `docs/adr/0001` through `docs/adr/0008`
- `docs/broker-capability-matrix.md`
- `.github/workflows/ci.yml` and `benchmark.yml`
- `docker/docker-compose.yml` (PostgreSQL, Redis, RabbitMQ, Kafka, NATS, LocalStack)

---

### M2 — Core Contracts + Runtime + InMemory Transport

**Status:** In Progress

Implement the zero-dependency abstractions package, core runtime (pipeline, capability engine, serialization, retry, scheduling), PostgreSQL outbox/inbox, request/response, the shared transport compatibility test suite, and InMemory transport. InMemory must pass the full compatibility suite, which gates every subsequent adapter.

**Deliverables:**
- `EventMesh.Abstractions` — `IMessageBus`, `IBrokerTransport`, `BrokerCapabilities`, envelope, filters
- `EventMesh.Core` — pipeline, capability engine, JSON serialization, retry/DLQ semantics
- `EventMesh.Storage.PostgreSql` — outbox/inbox with idempotency
- `EventMesh.Transport.InMemory` — passes compatibility suite
- `EventMesh.Transport.Compatibility.Tests` — shared contract tests
- BenchmarkDotNet harness with baseline throughput/latency metrics

---

### M3 — RabbitMQ Adapter

**Status:** Pending

Production RabbitMQ transport with topology provisioning, delayed-exchange detection, priority queues, and publisher confirms.

**Deliverables:**
- `EventMesh.Transport.RabbitMQ` passing the compatibility suite
- Integration tests via Testcontainers
- Broker-specific configuration and topology documentation

---

### M4 — Kafka Adapter

**Status:** Pending

Kafka transport using Confluent client with consumer groups, offset-based replay, and schema registry integration.

**Deliverables:**
- `EventMesh.Transport.Kafka` passing the compatibility suite
- Partition-aware publishing and consumer group management
- `EventMesh.SchemaRegistry` integration for Avro/Protobuf schemas

---

### M5 — Redis Streams Adapter

**Status:** Pending

Redis Streams transport with consumer groups and `XAUTOCLAIM` recovery for pending messages.

**Deliverables:**
- `EventMesh.Transport.RedisStreams` passing the compatibility suite
- Stream topology management and pending message recovery

---

### M6 — Azure Service Bus Adapter

**Status:** Pending

Azure Service Bus transport with sessions, native scheduling, transactions, and dead-letter handling.

**Deliverables:**
- `EventMesh.Transport.AzureServiceBus` passing the compatibility suite
- Session-aware consumers and scheduled message support

---

### M7 — AWS SQS Adapter

**Status:** Pending

AWS SQS transport supporting FIFO and standard queues, redrive dead-letter policies, and visibility timeout management. LocalStack used for CI integration tests.

**Deliverables:**
- `EventMesh.Transport.AmazonSqs` passing the compatibility suite
- FIFO deduplication and delay queue support (≤15 min native; scheduler for longer delays)

---

### M8 — Google Pub/Sub Adapter

**Status:** Pending

Google Cloud Pub/Sub transport with ordering keys and seek-based replay.

**Deliverables:**
- `EventMesh.Transport.GooglePubSub` passing the compatibility suite
- Ordering key support and snapshot/seek replay

---

### M9 — NATS JetStream Adapter

**Status:** Pending

NATS JetStream transport with durable consumers and sequence/time-based replay.

**Deliverables:**
- `EventMesh.Transport.Nats` passing the compatibility suite
- JetStream stream and consumer provisioning

---

### M10 — Dashboard

**Status:** Complete

Management API, SignalR hub, and React dashboard for operational visibility.

**Deliverables:**
- `EventMesh.Management.Api` — REST API and SignalR hub
- React dashboard (overview, connections, topics, queues, messages, consumers, retries, dead letters, replay, metrics, tracing, plugins, settings, cluster health)
- Playwright end-to-end tests

---

### M11 — CLI

**Status:** Pending

`eventmesh` command-line tool for publish, subscribe, replay, queue/topic inspection, plugin management, health checks, and benchmarks.

**Deliverables:**
- `EventMesh.Cli` published as a .NET global tool
- Commands: `publish`, `subscribe`, `replay`, `queues`, `topics`, `plugins`, `health`, `benchmark`

---

### M12 — SDK Polish

**Status:** Pending

Production-ready NuGet packaging, Roslyn analyzers, source-linked documentation, and sample applications.

**Deliverables:**
- NuGet packages for all public libraries with semantic versioning
- Analyzer package for configuration and handler conventions
- Sample projects demonstrating each broker and reliability pattern

---

### M13 — Plugin System

**Status:** Pending

Plugin SDK and first-party plugins for compression, encryption, and observability exporters.

**Deliverables:**
- `EventMesh.Plugin.Sdk` with `IEventMeshPlugin` contract and manifest format
- First-party plugins: gzip/zstd compression, AES-GCM encryption with Vault/AWS/Azure secret providers, metrics/tracing exporters
- `AssemblyLoadContext` plugin discovery for dashboard and CLI

---

### M14 — Performance

**Status:** Pending

Optimize hot paths for throughput and latency; publish BenchmarkDotNet results on every release.

**Deliverables:**
- Automatic batching, backpressure, connection pooling, and zero-copy serialization paths
- Published benchmarks: 100,000+ msg/s in-memory, P95 < 5 ms
- Performance tuning guide

---

### M15 — Production Hardening

**Status:** Pending

Security, deployment artifacts, chaos testing, and documentation for enterprise production use.

**Deliverables:**
- OAuth2/OIDC/JWT/API key authentication and RBAC on management API
- TLS configuration for all network endpoints
- Helm chart, Kubernetes manifests, Terraform modules
- Chaos tests and threat model documentation
- Migration and deployment guides
- 90%+ test coverage gate in CI

---

## Definition of Done

EventMesh reaches general availability when:

1. All seven broker adapters pass the shared compatibility test suite through the same public API
2. Applications can switch brokers via configuration without code changes
3. Dashboard, CLI, and SDK are production-ready
4. Documentation, benchmarks, Docker Compose, and Kubernetes deployment are complete
5. CI/CD passes with no placeholder code or TODO comments
6. The repository is suitable for enterprise production and open-source adoption

## How to Follow Progress

- Watch [GitHub Releases](https://github.com/eventmesh/eventmesh/releases) for version tags
- Check [CHANGELOG.md](CHANGELOG.md) for per-release notes
- Review open [milestones](https://github.com/eventmesh/eventmesh/milestones) on GitHub
- Join the community on [Discord](https://discord.gg/eventmesh) (see [SUPPORT.md](SUPPORT.md))
