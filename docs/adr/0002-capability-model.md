# ADR-0002: Capability Model with Emulation

## Status

Accepted

## Date

2026-07-08

## Context

The seven supported message brokers have fundamentally different feature sets. RabbitMQ supports priority queues; Kafka does not. Kafka supports offset-based replay; AWS SQS does not. Azure Service Bus has native scheduling; Redis Streams does not.

EventMesh promises a unified API (`PublishAsync`, `ScheduleAsync`, `RequestAsync`, `ReplayAsync`) regardless of broker. We must decide how to handle feature gaps:

1. Silently degrade behavior (unacceptable — surprises in production)
2. Fail at runtime when an unsupported feature is invoked (late failure, hard to debug)
3. Fail at configuration time with clear guidance (early failure)
4. Emulate missing features where a safe alternative exists

## Decision

Each transport declares its capabilities via a `[Flags]` enum `BrokerCapabilities` at registration time. The capability engine in `EventMesh.Core` applies three rules:

### Rule 1 — Native passthrough

When the broker natively supports a feature, the transport uses it directly. Example: Kafka offset seek for `ReplayAsync`.

### Rule 2 — Safe emulation

When emulation is possible without violating delivery guarantees, the runtime provides the feature:

| Missing capability | Emulation strategy |
|--------------------|--------------------|
| Delayed delivery | PostgreSQL-backed scheduler polls and publishes due messages |
| Dead letter queue | Convention-based error topic/queue (`{name}.error`) |
| Replay | Outbox archive table with timestamp queries |
| Priority | Multiple destination routing in core pipeline |
| Consumer groups | Competing consumers on shared destination (RabbitMQ) |

Emulation is transparent to application code but documented in the [broker capability matrix](../broker-capability-matrix.md).

### Rule 3 — Configuration-time rejection

When emulation is impossible or would violate semantics, the application fails at startup with `CapabilityNotSupportedException`:

```csharp
// This fails at startup when using AWS SQS:
services.AddEventMesh()
    .UseAmazonSqs(...)
    .EnableReplay(); // CapabilityNotSupportedException
```

Runtime invocation of unsupported features is never reached.

## Capability Declaration

Transports implement `IBrokerTransport.GetCapabilities()` returning their flags. The core runtime cross-references declared capabilities against the application's enabled features during `AddEventMesh()` configuration.

## Consequences

### Positive

- Developers get clear, actionable errors at startup instead of silent failures
- Safe emulation provides a consistent API across brokers for common patterns
- Capability matrix is machine-readable and drives compatibility tests
- Transports can declare partial support (e.g., SQS delay ≤ 15 minutes)

### Negative

- Emulation adds latency and infrastructure dependencies (PostgreSQL for scheduler)
- Emulated features may have different semantics than native (documented but requires awareness)
- Capability enum must evolve carefully as new features are added

### Neutral

- Compatibility test suite validates that declared capabilities match actual behavior
- Dashboard will display capability badges per connected broker (Milestone 10)

## References

- [Broker Capability Matrix](../broker-capability-matrix.md)
- [ADR-0005: Reliability](0005-reliability.md)
