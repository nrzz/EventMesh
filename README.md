# EventMesh

[![Build](https://img.shields.io/github/actions/workflow/status/eventmesh/eventmesh/ci.yml?branch=main&label=build)](https://github.com/eventmesh/eventmesh/actions/workflows/ci.yml)
[![Benchmarks](https://img.shields.io/github/actions/workflow/status/eventmesh/eventmesh/benchmark.yml?branch=main&label=benchmarks)](https://github.com/eventmesh/eventmesh/actions/workflows/benchmark.yml)
[![NuGet](https://img.shields.io/nuget/v/EventMesh.Core.svg)](https://www.nuget.org/packages/EventMesh.Core)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![Discord](https://img.shields.io/badge/Discord-join-5865F2)](https://discord.gg/eventmesh)
[![Docs](https://img.shields.io/badge/docs-architecture-0A7ACA)](ARCHITECTURE.md)

**EventMesh** is a broker-agnostic distributed messaging platform for .NET. Write your application once against a unified API and deploy against RabbitMQ, Kafka, Redis Streams, Azure Service Bus, AWS SQS, Google Pub/Sub, or NATS JetStream without changing business logic.

EventMesh combines the developer experience of MassTransit and NServiceBus with the interoperability of CloudEvents, the observability of OpenTelemetry, and a capability-driven emulation engine that bridges broker differences transparently.

## Features

- **Unified messaging API** — `PublishAsync`, `ScheduleAsync`, `RequestAsync`, `ReplayAsync`, and `SubscribeAsync` on a single `IMessageBus` abstraction
- **Seven production transports** — RabbitMQ, Kafka, Redis Streams, Azure Service Bus, AWS SQS, Google Pub/Sub, NATS JetStream, plus InMemory for tests and benchmarks
- **Capability model** — Each broker declares supported features; the runtime emulates missing capabilities where safe
- **Reliability patterns** — Transactional outbox, idempotent inbox, retry policies, dead-letter queues, and saga support
- **CloudEvents envelope** — Canonical wire format with pluggable serializers (JSON, MessagePack, Protobuf, Avro)
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

### Start local infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Configure and publish

```csharp
using EventMesh.Core;
using EventMesh.Transport.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddEventMesh(options =>
    {
        options.UseRabbitMq(rabbit =>
        {
            rabbit.Host = "localhost";
            rabbit.Port = 5672;
            rabbit.Username = "eventmesh";
            rabbit.Password = "eventmesh";
        });
    })
    .AddMessageHandler<OrderCreatedHandler>();

var app = builder.Build();
await app.RunAsync();

// In your application code:
await bus.PublishAsync(new OrderCreated(orderId, customerId));

await bus.ScheduleAsync(new PaymentReminder(orderId), TimeSpan.FromMinutes(5));

var response = await bus.RequestAsync<CreateOrder, OrderResponse>(new CreateOrder(items));

await bus.ReplayAsync("orders.created", from: DateTimeOffset.UtcNow.AddHours(-1));
```

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

| Broker | Package | Native strengths |
|--------|---------|------------------|
| RabbitMQ | `EventMesh.Transport.RabbitMQ` | Routing keys, priority, TTL, delayed exchange |
| Apache Kafka | `EventMesh.Transport.Kafka` | Partitions, consumer groups, offset replay |
| Redis Streams | `EventMesh.Transport.RedisStreams` | Consumer groups, pending/claim recovery |
| Azure Service Bus | `EventMesh.Transport.AzureServiceBus` | Sessions, transactions, native scheduling |
| AWS SQS | `EventMesh.Transport.AmazonSqs` | FIFO queues, delay queues, redrive DLQ |
| Google Pub/Sub | `EventMesh.Transport.GooglePubSub` | Ordering keys, seek-based replay |
| NATS JetStream | `EventMesh.Transport.Nats` | Durable consumers, sequence/time replay |

See the [Broker Capability Matrix](docs/broker-capability-matrix.md) for the full feature comparison and emulation behavior.

## Repository Layout

```
EventMesh/
├── src/                    # Core libraries and transport adapters
├── tests/                  # Unit, integration, and compatibility tests
├── benchmarks/               # BenchmarkDotNet performance suite
├── cli/                    # eventmesh CLI tool
├── sdk/                    # Plugin SDK
├── dashboard/              # React management UI (Milestone 10)
├── docker/                 # Local development infrastructure
├── docs/                   # Architecture docs and ADRs
└── .github/workflows/      # CI and benchmark automation
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
