# ADR-0004: CloudEvents Envelope

## Status

Accepted

## Date

2026-07-08

## Context

EventMesh supports seven message brokers with different wire formats: AMQP properties (RabbitMQ), byte records with headers (Kafka), Redis hash fields (Redis Streams), broker-specific SDK types (Azure, AWS, Google, NATS).

Applications should not depend on broker-specific message formats. We need a canonical envelope that:

- Provides a consistent structure across all transports
- Supports interoperability with non-EventMesh systems
- Carries metadata for tracing, scheduling, and idempotency
- Allows pluggable payload serialization

## Decision

All messages use [CloudEvents 1.0](https://cloudevents.io/) as the canonical wire format.

### Required attributes

| Attribute | Purpose |
|-----------|---------|
| `id` | Unique message identifier; used for inbox deduplication |
| `source` | URI identifying the publishing service |
| `type` | Event type (CLR type name or custom string) |
| `time` | UTC timestamp of creation |
| `datacontenttype` | MIME type of the data payload |

### EventMesh extension attributes

| Extension | Purpose |
|-----------|---------|
| `correlationid` | Distributed tracing correlation across services |
| `causationid` | ID of the message that caused this message |
| `scheduletime` | UTC time for scheduled delivery |
| `retrycount` | Current retry attempt number |
| `priority` | Message priority (0–255) |
| `partitionkey` | Key for partition/ordering routing |

### Serialization modes

- **Structured content mode** — Full CloudEvent as JSON in the message body. Used by RabbitMQ, Redis Streams, SQS.
- **Binary content mode** — Payload in body; CloudEvent attributes in transport headers/properties. Used by Kafka, NATS, Azure Service Bus, Google Pub/Sub.

The serialization filter selects the mode based on transport capabilities.

### Payload serializers

Default serializer is `System.Text.Json`. Pluggable serializers registered via DI:

| Serializer | Content Type | Use Case |
|------------|-------------|----------|
| System.Text.Json | `application/json` | Default; human-readable |
| MessagePack | `application/msgpack` | Compact binary |
| Google.Protobuf | `application/protobuf` | Schema-driven contracts |
| Apache.Avro | `application/avro` | Schema registry integration |

Serializer selection is per-message-type or global default, configured in `AddEventMesh()`.

### Schema registry integration

When `EventMesh.SchemaRegistry` is enabled, the `type` attribute maps to a registered schema version. Compatibility checks (backward, forward, full) run at publish time.

## Consequences

### Positive

- Consistent message structure across all brokers
- Interoperability with CloudEvents-native systems (Knative, Dapr, Azure Event Grid)
- Extension attributes provide a standard location for cross-cutting metadata
- Binary and structured modes optimize for each transport's strengths

### Negative

- CloudEvents overhead (attribute mapping) adds bytes to every message
- Binary mode requires careful header size management (Kafka header limits)
- Non-CloudEvents consumers require a translation layer

### Neutral

- Envelope type is `CloudEventEnvelope` in `EventMesh.Abstractions`
- Transport adapters are responsible for mapping between CloudEvents and broker-native formats

## References

- [CloudEvents Specification v1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
- [ADR-0003: Pipeline Architecture](0003-pipeline-architecture.md)
