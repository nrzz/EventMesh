# ADR-0005: Reliability (Outbox/Inbox/Retry)

## Status

Accepted

## Date

2026-07-08

## Context

Distributed messaging systems face inherent reliability challenges:

- Broker outages during publish can lose messages or leave application state inconsistent
- Broker redelivery can cause duplicate processing
- Transient handler failures need retry without losing messages
- Permanently failing messages must be isolated without blocking the queue

EventMesh must provide enterprise-grade reliability patterns that work consistently across all seven brokers, most of which offer only at-least-once delivery.

## Decision

EventMesh implements three complementary reliability patterns backed by PostgreSQL.

### Transactional Outbox

When enabled, `PublishAsync` persists messages to an outbox table within the same database transaction as application state changes:

1. Application begins a database transaction
2. Application modifies business state
3. `PublishAsync` writes the CloudEvent envelope to the `eventmesh_outbox` table
4. Transaction commits atomically
5. Background `OutboxDispatcher` polls pending entries and publishes to the broker
6. Successfully dispatched entries are marked as sent

This guarantees that a message is never published without the corresponding state change, and vice versa.

### Idempotent Inbox

Consumers record processed message IDs in an `eventmesh_inbox` table:

1. Message arrives from broker
2. Inbox filter checks if `envelope.id` exists in inbox table
3. If duplicate: acknowledge without executing handler
4. If new: insert inbox record, execute handler, acknowledge on success

This provides exactly-once *processing* (not delivery) regardless of broker delivery guarantees.

### Retry and Dead Letter

Failed handler executions are retried with configurable policies:

```csharp
services.AddEventMesh()
    .ConfigureRetry(retry => retry
        .MaxAttempts(5)
        .Backoff(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5))
        .Jitter(0.2));
```

Retry behavior:
1. Handler throws exception
2. Retry filter increments `retrycount` in envelope
3. If under max attempts: message is requeued with computed delay (native or emulated)
4. If max attempts exceeded: message moves to dead-letter destination

Dead-letter routing:
- **Native DLQ** — when broker supports it (RabbitMQ, Azure SB, SQS, Pub/Sub, NATS)
- **Emulated DLQ** — convention-based error destination (Kafka, Redis Streams)

### Delivery guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| At-least-once delivery | Default; broker redelivery + outbox redispatch |
| At-most-once delivery | Opt-in; acknowledge before handler (not recommended) |
| Exactly-once processing | Inbox deduplication + outbox (not broker-native) |

### Storage implementation

- **Technology:** PostgreSQL via Dapper/Npgsql (no EF Core on hot path)
- **Tables:** `eventmesh_outbox`, `eventmesh_inbox`, `eventmesh_schedule`
- **Indexes:** `(status, created_at)` on outbox; `(message_id)` unique on inbox
- **Cleanup:** Configurable retention policy for processed inbox entries and sent outbox entries

## Consequences

### Positive

- Consistent reliability semantics across all brokers
- Outbox pattern is well-understood in enterprise .NET (NServiceBus, MassTransit precedent)
- Inbox deduplication is simple and effective for idempotent processing
- PostgreSQL is already required for scheduler emulation

### Negative

- PostgreSQL becomes a hard dependency for full reliability features
- Outbox adds latency (async dispatch) compared to direct publish
- Inbox table grows and requires retention management
- Exactly-once processing requires handlers to be idempotent for side effects beyond the inbox check

### Neutral

- Reliability features are opt-in per endpoint or globally
- Direct publish (no outbox) remains available for latency-sensitive, loss-tolerant scenarios

## References

- [ADR-0002: Capability Model](0002-capability-model.md)
- [Broker Capability Matrix](../broker-capability-matrix.md)
