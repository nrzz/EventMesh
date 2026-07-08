# ADR-0006: Observability-First

## Status

Accepted

## Date

2026-07-08

## Context

Distributed messaging is inherently difficult to debug: messages cross process boundaries, brokers introduce async delays, and failures may manifest far from the root cause. EventMesh targets enterprise production use where observability is not optional.

The framework must provide tracing, metrics, and structured logging out of the box without requiring application developers to instrument each handler manually.

## Decision

Every pipeline filter and transport operation emits three telemetry signals by default.

### Distributed tracing (OpenTelemetry)

- **Instrumentation:** OpenTelemetry .NET SDK with messaging semantic conventions
- **Span naming:** `eventmesh publish {destination}`, `eventmesh consume {source}`
- **Attributes:** `messaging.system`, `messaging.destination`, `messaging.message_id`, `messaging.operation`
- **Propagation:** W3C Trace Context (`traceparent`, `tracestate`) injected into CloudEvents extension attributes and transport headers
- **Export:** OTLP exporter (default); configurable to Jaeger, Zipkin, Azure Monitor, etc.

### Metrics (System.Diagnostics.Metrics)

| Metric | Type | Description |
|--------|------|-------------|
| `eventmesh.messages.published` | Counter | Total messages published |
| `eventmesh.messages.consumed` | Counter | Total messages consumed |
| `eventmesh.messages.failed` | Counter | Handler failures |
| `eventmesh.messages.retried` | Counter | Retry attempts |
| `eventmesh.messages.dead_lettered` | Counter | Messages sent to DLQ |
| `eventmesh.publish.duration` | Histogram | Publish pipeline latency |
| `eventmesh.consume.duration` | Histogram | Consume pipeline latency |
| `eventmesh.outbox.pending` | Gauge | Unsent outbox entries |
| `eventmesh.inbox.duplicates` | Counter | Duplicate messages skipped |

- **Export:** Prometheus endpoint via `OpenTelemetry.Exporter.Prometheus.AspNetCore` on management API; `System.Diagnostics.Metrics` listener for in-process scenarios

### Structured logging

- **Framework:** `Microsoft.Extensions.Logging` with structured message templates
- **Fields:** `MessageId`, `CorrelationId`, `CausationId`, `MessageType`, `Destination`, `Broker`, `RetryCount`, `DurationMs`
- **Levels:** Information for publish/consume; Warning for retries; Error for failures and dead-lettering
- **Payload policy:** Message bodies are never logged by default; opt-in via `LogPayloadContent` configuration (development only)

### Correlation and causation

- `correlationid` — set on the first message in a workflow; propagated unchanged across all subsequent messages
- `causationid` — set to the `id` of the message that directly caused the current message
- Both are CloudEvents extension attributes, ensuring propagation across broker boundaries

### Health checks

`Microsoft.Extensions.Diagnostics.HealthChecks` integration:

- Broker connectivity (can connect and declare topology)
- PostgreSQL outbox/inbox connectivity
- Outbox dispatcher lag (pending entries older than threshold)
- Consumer lag (where broker supports it)

## Consequences

### Positive

- Production debugging is possible from day one without custom instrumentation
- OpenTelemetry alignment enables integration with existing observability stacks
- Correlation/causation IDs enable end-to-end workflow tracing
- Metrics drive dashboard visualizations (Milestone 10)

### Negative

- Telemetry adds overhead to the hot path (mitigated by async export and sampling)
- High-cardinality metrics (per-destination) can stress Prometheus at scale
- Log volume increases; requires log aggregation infrastructure

### Neutral

- Sampling is configurable via standard OpenTelemetry sampler configuration
- Telemetry filters can be removed from the pipeline for benchmark runs

## References

- [OpenTelemetry Messaging Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/)
- [ADR-0003: Pipeline Architecture](0003-pipeline-architecture.md)
- [ADR-0008: Control Plane vs Data Plane](0008-control-plane-vs-data-plane.md)
