# Broker Capability Matrix

This document defines the messaging capabilities of each supported broker and how EventMesh handles features that are not natively available. The capability engine uses this matrix to decide whether to use native broker features, emulate via the runtime, or reject at configuration time.

**Legend:**
- **Native** — Broker provides the feature directly
- **Emulated** — EventMesh runtime provides the feature (e.g., PostgreSQL scheduler, outbox archive)
- **Unsupported** — Feature cannot be provided; `CapabilityNotSupportedException` at configuration time
- **Partial** — Supported with broker-specific limitations

## Capability Summary

| Capability | RabbitMQ | Kafka | Redis Streams | Azure Service Bus | AWS SQS | Google Pub/Sub | NATS JetStream |
|------------|:--------:|:-----:|:-------------:|:-----------------:|:-------:|:--------------:|:--------------:|
| Publish/Subscribe | Native | Native | Native | Native | Native | Native | Native |
| Routing Keys | Native | Unsupported | Unsupported | Unsupported | Unsupported | Unsupported | Unsupported |
| Topic/Queue Topology | Native | Native | Native | Native | Native | Native | Native |
| Consumer Groups | Emulated | Native | Native | Emulated | Emulated | Native | Native |
| Partitions | Unsupported | Native | Unsupported | Unsupported | Unsupported | Unsupported | Unsupported |
| Ordering (per key) | Partial | Native | Partial | Native (sessions) | Native (FIFO) | Native | Partial |
| Priority Queues | Native | Unsupported | Unsupported | Native | Unsupported | Unsupported | Emulated |
| Delayed Delivery | Native¹ | Emulated | Emulated | Native | Partial² | Emulated | Emulated |
| Scheduled Messages | Emulated | Emulated | Emulated | Native | Partial² | Emulated | Emulated |
| Dead Letter Queue | Native | Emulated | Emulated | Native | Native³ | Native | Native⁴ |
| Message TTL | Native | Emulated | Emulated | Native | Native | Emulated | Native |
| Replay | Emulated⁵ | Native | Native | Unsupported | Unsupported | Native | Native |
| Request/Response | Emulated | Emulated | Emulated | Native | Emulated | Emulated | Native |
| Sessions | Unsupported | Unsupported | Unsupported | Native | Unsupported | Unsupported | Unsupported |
| Transactions | Unsupported | Native | Unsupported | Native | Unsupported | Unsupported | Unsupported |
| Publisher Confirms | Native | Native | Native | Native | Native | Native | Native |
| Batching | Emulated | Native | Emulated | Emulated | Emulated | Emulated | Emulated |
| Compression | Emulated | Native | Emulated | Emulated | Emulated | Emulated | Emulated |
| Exactly-Once Processing | Emulated⁶ | Emulated⁶ | Emulated⁶ | Emulated⁶ | Emulated⁶ | Emulated⁶ | Emulated⁶ |
| Visibility Timeout | Unsupported | Unsupported | Emulated | Native | Native | Unsupported | Emulated |
| Idempotency Keys | Emulated | Emulated | Emulated | Native | Native (FIFO) | Emulated | Emulated |

¹ Requires `rabbitmq_delayed_message_exchange` plugin; auto-detected at startup.
² Native delay up to 15 minutes; longer delays use PostgreSQL scheduler.
³ Via SQS redrive policy to a configured DLQ ARN.
⁴ Via `max_deliver` advisory and dead-letter stream.
⁵ Emulated via outbox archive table with timestamp-based replay queries.
⁶ Achieved via inbox deduplication + outbox; not broker-native exactly-once.

## Broker Profiles

### RabbitMQ

**Package:** `EventMesh.Transport.RabbitMQ`

| Feature | Support | Notes |
|---------|---------|-------|
| Routing keys | Native | Topic and direct exchanges with binding keys |
| Priority | Native | 0–255 priority levels via `x-max-priority` queue argument |
| TTL | Native | Per-message and per-queue TTL |
| Delayed delivery | Native | Via delayed message exchange plugin; falls back to scheduler if plugin absent |
| DLQ | Native | Dead-letter exchange and routing key configuration |
| Publisher confirms | Native | Confirms enabled by default; timeout configurable |
| Replay | Emulated | No native replay; outbox archive enables time-range replay |
| Consumer groups | Emulated | Competing consumers on shared queue (not Kafka-style groups) |

**Recommended for:** Traditional enterprise messaging, complex routing, priority workloads.

### Apache Kafka

**Package:** `EventMesh.Transport.Kafka`

| Feature | Support | Notes |
|---------|---------|-------|
| Partitions | Native | Key-based partition assignment |
| Consumer groups | Native | Full offset management and rebalancing |
| Replay | Native | Offset-based seek to any point in retention |
| Ordering | Native | Per-partition ordering guaranteed |
| Transactions | Native | Idempotent producer and transactional consume-process-produce |
| DLQ | Emulated | Error topic via convention; no native DLQ |
| Delay | Emulated | PostgreSQL scheduler; no native delay |
| Priority | Unsupported | No priority queue concept |

**Recommended for:** High-throughput event streaming, event sourcing, log-based replay.

### Redis Streams

**Package:** `EventMesh.Transport.RedisStreams`

| Feature | Support | Notes |
|---------|---------|-------|
| Consumer groups | Native | `XREADGROUP` with consumer name |
| Pending recovery | Native | `XAUTOCLAIM` for stale pending messages |
| Replay | Native | `XRANGE` by stream ID |
| Ordering | Partial | Per-stream ordering; no cross-stream ordering |
| DLQ | Emulated | Error stream via convention |
| Delay | Emulated | PostgreSQL scheduler |
| Priority | Emulated | Multiple streams with priority routing in core |

**Recommended for:** Lightweight streaming, low-latency workloads, existing Redis infrastructure.

### Azure Service Bus

**Package:** `EventMesh.Transport.AzureServiceBus`

| Feature | Support | Notes |
|---------|---------|-------|
| Sessions | Native | FIFO within session; session lock management |
| Scheduling | Native | `ScheduledEnqueueTimeUtc` for future delivery |
| Transactions | Native | Atomic complete/defer/send in transaction scope |
| DLQ | Native | Built-in dead-letter sub-queue |
| TTL | Native | `TimeToLive` on messages |
| Priority | Native | Via dedicated priority queues |
| Replay | Unsupported | No native message replay; use outbox archive for limited replay |
| Request/response | Native | Reply-to session and correlation ID |

**Recommended for:** Azure-native applications, enterprise messaging with sessions and scheduling.

### AWS SQS

**Package:** `EventMesh.Transport.AmazonSqs`

| Feature | Support | Notes |
|---------|---------|-------|
| FIFO | Native | Deduplication ID and message group ID |
| Standard | Native | At-least-once, best-effort ordering |
| Delay | Partial | Native delay 0–900 seconds; longer via scheduler |
| Visibility timeout | Native | Configurable per queue; heartbeat extension supported |
| DLQ | Native | Redrive policy to configured DLQ |
| Replay | Unsupported | Messages deleted after processing; no seek |
| Priority | Unsupported | No priority concept |
| Batching | Emulated | `SendMessageBatch` / `ReceiveMessage` batching in adapter |

**Recommended for:** AWS-native serverless and microservice architectures, FIFO workflows.

### Google Pub/Sub

**Package:** `EventMesh.Transport.GooglePubSub`

| Feature | Support | Notes |
|---------|---------|-------|
| Ordering keys | Native | Per-key ordering within a region |
| Replay | Native | Seek to timestamp or snapshot |
| DLQ | Native | Dead-letter topic via subscription policy |
| Push/Pull | Native | Pull by default; push via webhook adapter (future) |
| Delay | Emulated | PostgreSQL scheduler |
| Priority | Unsupported | No native priority |

**Recommended for:** GCP-native event-driven architectures, global fan-out.

### NATS JetStream

**Package:** `EventMesh.Transport.Nats`

| Feature | Support | Notes |
|---------|---------|-------|
| Durable consumers | Native | Consumer persistence across restarts |
| Replay | Native | By sequence number or timestamp |
| DLQ | Native | Max deliver count with advisory notification |
| Request/response | Native | Core NATS request-reply pattern |
| Ordering | Partial | Per-stream ordering |
| Priority | Emulated | Multiple streams with priority routing |
| Delay | Emulated | PostgreSQL scheduler |

**Recommended for:** Cloud-native microservices, edge deployments, low-latency pub/sub.

## Emulation Details

### PostgreSQL Scheduler (Delayed Delivery)

When a broker lacks native delayed delivery, EventMesh persists scheduled messages in a PostgreSQL table. A background service polls for due messages and publishes them through the normal pipeline. Used by: Kafka, Redis Streams, Google Pub/Sub, NATS, and RabbitMQ (when delayed exchange plugin is absent).

### Outbox Archive (Replay)

When a broker lacks native replay, EventMesh can archive published messages in the outbox table with timestamps. `ReplayAsync` queries the archive and re-publishes matching messages. Used by: RabbitMQ. Limited compared to native offset/seek replay.

### Convention-Based DLQ

For brokers without native dead-letter support, EventMesh routes failed messages to a conventionally named error destination (e.g., `{original-topic}.error` or `{queue-name}-dlq`). Used by: Kafka, Redis Streams.

### Inbox Deduplication (Exactly-Once Processing)

All brokers achieve exactly-once *processing* (not delivery) via the inbox pattern: message IDs are recorded in PostgreSQL before handler execution. Duplicate deliveries are acknowledged without re-processing.

## Compatibility Test Suite

Every transport adapter must pass the shared test suite in `tests/EventMesh.Transport.Compatibility.Tests/`. The suite validates:

- Publish and subscribe round-trip
- Message serialization and CloudEvents envelope integrity
- Retry and dead-letter behavior
- Request/response correlation
- Scheduled message delivery (native or emulated)
- Consumer group behavior (where applicable)
- Graceful shutdown and message acknowledgment
- Capability declaration accuracy

## Configuration Validation

At startup, EventMesh validates the application's requested features against the transport's declared `BrokerCapabilities`. Misconfiguration produces a clear error:

```
CapabilityNotSupportedException: Replay is not supported by EventMesh.Transport.AmazonSqs.
Remove ReplayAsync usage or switch to a transport that supports replay (Kafka, Redis Streams, Google Pub/Sub, NATS JetStream).
```

## Related Documents

- [ARCHITECTURE.md](../ARCHITECTURE.md) — Capability engine design
- [ADR-0002: Capability Model](adr/0002-capability-model.md) — Architectural decision
- [ROADMAP.md](../ROADMAP.md) — Transport adapter delivery milestones
