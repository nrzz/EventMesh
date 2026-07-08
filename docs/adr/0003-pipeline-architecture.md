# ADR-0003: Pipeline Architecture

## Status

Accepted

## Date

2026-07-08

## Context

EventMesh must support cross-cutting concerns (serialization, compression, encryption, retry, outbox, tracing, metrics) that apply uniformly across all transports. Hard-coding these concerns into `EventMesh.Core` or individual transports would create duplication and make the system difficult to extend.

MassTransit, ASP.NET Core middleware, and gRPC interceptors demonstrate that composable filter pipelines are a proven pattern for messaging frameworks.

## Decision

Publish and consume operations flow through composable middleware filter pipelines, similar to ASP.NET Core middleware and MassTransit filters.

### Pipeline structure

```
Publish:  App → [Serialize] → [Compress] → [Encrypt] → [Outbox] → [Trace] → [Metrics] → Transport
Consume:  Transport → [Metrics] → [Trace] → [Inbox] → [Decrypt] → [Decompress] → [Deserialize] → [Retry] → Handler
```

### Filter contract

```csharp
public interface IMessageFilter
{
    Task PublishAsync(PublishContext context, FilterDelegate next, CancellationToken cancellationToken);
    Task ConsumeAsync(ConsumeContext context, FilterDelegate next, CancellationToken cancellationToken);
}
```

Filters are registered via DI and ordered by `FilterOrder` attribute or explicit registration sequence.

### Built-in filters

| Filter | Order | Phase | Responsibility |
|--------|-------|-------|----------------|
| OpenTelemetry | First | Both | Create and propagate spans |
| Serialization | Early | Both | CloudEvents encode/decode |
| Compression | After serialize | Both | gzip/zstd payload compression |
| Encryption | After compress | Both | AES-GCM payload encryption |
| Outbox | Before transport | Publish | Persist to PostgreSQL outbox |
| Inbox | After transport | Consume | Deduplicate via inbox table |
| Retry | Before handler | Consume | Exponential backoff on failure |
| Metrics | Last | Both | Record counters and histograms |

### Plugin integration

Plugins implement `IEventMeshPlugin` and register filters via:

```csharp
public void Configure(IFilterRegistration registration)
{
    registration.AddPublishFilter<MyPublishFilter>();
    registration.AddConsumeFilter<MyConsumeFilter>();
}
```

### Context objects

- `PublishContext` — message, envelope, transport destination, cancellation token, bag for filter state
- `ConsumeContext` — received envelope, handler type, retry count, acknowledgment callbacks

## Consequences

### Positive

- Cross-cutting concerns are implemented once and apply to all transports
- Plugins extend the pipeline without modifying core code
- Filter ordering is explicit and testable
- Individual filters can be unit tested in isolation

### Negative

- Pipeline adds indirection; hot-path performance must be validated via benchmarks
- Filter ordering bugs can be subtle (e.g., decrypt before decompress)
- Deep filter chains increase stack depth

### Neutral

- Default pipeline is configured by `AddEventMesh()` with sensible defaults
- Advanced users can remove or reorder filters explicitly

## References

- [ADR-0004: CloudEvents Envelope](0004-cloudevents-envelope.md)
- [ADR-0006: Observability-First](0006-observability-first.md)
- [ADR-0007: Plugin System](0007-plugin-system.md)
