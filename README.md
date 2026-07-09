# EventMesh

[![Build](https://img.shields.io/github/actions/workflow/status/nrzz/EventMesh/ci.yml?branch=main&label=build)](https://github.com/nrzz/EventMesh/actions/workflows/ci.yml)
[![Benchmarks](https://img.shields.io/github/actions/workflow/status/nrzz/EventMesh/benchmark.yml?branch=main&label=benchmarks)](https://github.com/nrzz/EventMesh/actions/workflows/benchmark.yml)
[![NuGet](https://img.shields.io/nuget/v/EventMesh.Core.svg?label=NuGet)](https://www.nuget.org/packages/EventMesh.Core/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![Docs](https://img.shields.io/badge/docs-architecture-0A7ACA)](ARCHITECTURE.md)

**EventMesh** is a broker-agnostic distributed messaging platform for .NET. Write your application once against a unified API and deploy against RabbitMQ, Kafka, Redis Streams, Azure Service Bus, AWS SQS, Google Pub/Sub, or NATS JetStream without changing business logic.

EventMesh combines the developer experience of MassTransit and NServiceBus with the interoperability of CloudEvents, the observability of OpenTelemetry, and a capability-driven emulation engine that bridges broker differences transparently.

> **Project status:** EventMesh is under active development (v0.1.0). NuGet packages are published on [tagged releases](https://github.com/nrzz/EventMesh/releases). Broker adapters are **compatibility-tested (beta)** — see the [Broker Capability Matrix](docs/broker-capability-matrix.md) for current coverage.

## Features

- **Unified messaging API** — `PublishAsync`, `ScheduleAsync`, `RequestAsync`, `ReplayAsync`, and `SubscribeAsync` on a single `IMessageBus` abstraction
- **Seven broker adapters (compatibility-tested, beta)** — RabbitMQ, Kafka, Redis Streams, Azure Service Bus, AWS SQS, Google Pub/Sub, NATS JetStream, plus InMemory for tests and benchmarks
- **Capability model** — Each broker declares supported features; the runtime emulates missing capabilities where safe
- **Reliability patterns** — Transactional outbox, idempotent inbox, retry policies, dead-letter queues, and saga support
- **CloudEvents envelope** — Canonical wire format with pluggable serializers (JSON today; MessagePack, Protobuf, and Avro planned)
- **Observability-first** — OpenTelemetry traces, Prometheus metrics, structured logs with correlation and causation IDs
- **Control plane** — Optional management API, React dashboard, and `eventmesh` CLI for operations and visibility
- **Plugin architecture** — Extend serialization, compression, encryption, authentication, and exporters via versioned plugins

## Quick Start

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker](https://www.docker.com/) (for local brokers and integration tests)

### Install packages

```bash
dotnet add package EventMesh.Core
dotnet add package EventMesh.Transport.RabbitMQ
```

NuGet packages are published when a version tag (for example `v0.1.0`) is pushed. Until the first release, reference projects from this repository.

### Start local infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Configure, subscribe, and publish

EventMesh registers a transport factory, wires it through `UseTransport(...)`, and exposes `IMessageBus` from DI. The deferred factory pattern below matches the samples in `samples/BrokerSwitching` and avoids a host-build cycle:

```csharp
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Core;
using EventMesh.Transport.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var hostBuilder = Host.CreateApplicationBuilder();

// 1. Register the broker transport factory
hostBuilder.Services.AddRabbitMqTransport(options =>
{
    options.HostName = "localhost";
    options.Port = 5672;
    options.UserName = "eventmesh";
    options.Password = "eventmesh";
    options.VirtualHost = "eventmesh";
});

// 2. Register EventMesh and select the transport
IHost? host = null;
hostBuilder.Services.AddEventMesh(mesh =>
    mesh.UseTransport(new DeferredTransportFactory(
        () => host!.Services.GetRequiredService<RabbitMqTransportFactory>(),
        "rabbitmq")));

host = hostBuilder.Build();
await host.StartAsync();

// 3. Publish and consume
var bus = host.Services.GetRequiredService<IMessageBus>();

await using var consumer = await bus.SubscribeAsync<OrderCreated>(
    async (order, ct) => Console.WriteLine($"Order {order.OrderId}: {order.Amount:C}"),
    cancellationToken: CancellationToken.None);

await bus.PublishAsync(new OrderCreated(Guid.NewGuid(), 42.50m));

public sealed record OrderCreated(Guid OrderId, decimal Amount);

internal sealed class DeferredTransportFactory : IBrokerTransportFactory
{
    private readonly Func<IBrokerTransportFactory> _factoryResolver;
    private readonly string _transportName;

    public DeferredTransportFactory(Func<IBrokerTransportFactory> factoryResolver, string transportName)
    {
        _factoryResolver = factoryResolver;
        _transportName = transportName;
    }

    public string TransportName => _transportName;

    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default) =>
        _factoryResolver().CreateTransportAsync(settings, cancellationToken);
}
```

Optional helpers:

- `AddMessageHandler<THandler>()` registers a typed handler and auto-subscribes it at startup.
- `EnableOutbox()`, `EnableInbox()`, and `EnableReplay()` opt into reliability features on the fluent builder.

See `samples/BasicPublishSubscribe` (InMemory) and `samples/BrokerSwitching` (RabbitMQ) for runnable examples.

### Run tests

```bash
dotnet restore EventMesh.slnx
dotnet build EventMesh.slnx --configuration Release
dotnet test EventMesh.slnx --configuration Release
```

### Run benchmarks

```bash
dotnet run --project benchmarks/EventMesh.Benchmarks -c Release
```

## Supported Brokers

| Broker | Package | Status | Native strengths |
|--------|---------|--------|------------------|
| RabbitMQ | `EventMesh.Transport.RabbitMQ` | Compatibility-tested (beta) | Routing keys, priority, TTL, delayed exchange |
| Apache Kafka | `EventMesh.Transport.Kafka` | Compatibility-tested (beta) | Partitions, consumer groups, offset replay |
| Redis Streams | `EventMesh.Transport.RedisStreams` | Compatibility-tested (beta) | Consumer groups, pending/claim recovery |
| Azure Service Bus | `EventMesh.Transport.AzureServiceBus` | Compatibility-tested (beta) | Sessions, transactions, native scheduling |
| AWS SQS | `EventMesh.Transport.AmazonSqs` | Compatibility-tested (beta) | FIFO queues, delay queues, redrive DLQ |
| Google Pub/Sub | `EventMesh.Transport.GooglePubSub` | Compatibility-tested (beta) | Ordering keys, seek-based replay |
| NATS JetStream | `EventMesh.Transport.Nats` | Compatibility-tested (beta) | Durable consumers, sequence/time replay |
| InMemory | `EventMesh.Transport.InMemory` | Stable for tests | Zero external dependencies |

See the [Broker Capability Matrix](docs/broker-capability-matrix.md) for the full feature comparison and emulation behavior.

## Repository Layout

```
EventMesh/
├── src/                    # Core libraries and transport adapters
├── tests/                  # Unit, integration, and compatibility tests
├── benchmarks/             # BenchmarkDotNet performance suite
├── cli/                    # eventmesh CLI tool
├── sdk/                    # Plugin SDK
├── dashboard/              # React management UI (Milestone 10)
├── docker/                 # Local development infrastructure
├── docs/                   # Architecture docs and ADRs
└── .github/workflows/      # CI, release, and benchmark automation
```

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design, layering, data/control plane |
| [ROADMAP.md](ROADMAP.md) | Milestone plan and delivery status |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guidelines and PR process |
| [SECURITY.md](SECURITY.md) | Security policy and vulnerability reporting |
| [SUPPORT.md](SUPPORT.md) | Community and commercial support channels |
| [CHANGELOG.md](CHANGELOG.md) | Release history |
| [docs/adr/](docs/adr/) | Architecture Decision Records |

## Performance Targets

| Metric | Target |
|--------|--------|
| Throughput (InMemory) | 100,000+ messages/sec |
| P95 latency (InMemory) | < 5 ms |
| Test coverage | > 90% at GA |

Benchmark results are published on every release via the [benchmark workflow](.github/workflows/benchmark.yml).

## License

EventMesh is released under the [MIT License](LICENSE).

## Contributing

We welcome contributions. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and our [Code of Conduct](CODE_OF_CONDUCT.md) before opening a pull request.
