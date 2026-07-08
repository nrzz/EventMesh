# Changelog

All notable changes to EventMesh are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-08

### Added

- Initial repository scaffolding with .NET 10 (`net10.0`) solution structure
- Project skeletons for core messaging libraries:
  - `EventMesh.Abstractions` — zero-dependency contracts package
  - `EventMesh.Core` — runtime engine
  - `EventMesh.Transport.*` — RabbitMQ, Kafka, Redis Streams, Azure Service Bus, Amazon SQS, Google Pub/Sub, NATS, and InMemory adapters
  - `EventMesh.Storage.PostgreSql` and `EventMesh.Storage.Redis` — persistence layers
  - `EventMesh.SchemaRegistry`, `EventMesh.Security`, `EventMesh.Management.Api`
- CLI skeleton (`EventMesh.Cli`) and Plugin SDK skeleton (`EventMesh.Plugin.Sdk`)
- Test projects: unit, integration, and transport compatibility suites
- BenchmarkDotNet project (`EventMesh.Benchmarks`)
- Central package management via `Directory.Packages.props`
- Root documentation: README, ARCHITECTURE, ROADMAP, CONTRIBUTING, SECURITY, SUPPORT, CODE_OF_CONDUCT
- Architecture Decision Records ADR-0001 through ADR-0008
- Broker capability matrix documentation
- Docker Compose stack for local development (PostgreSQL, Redis, RabbitMQ, Kafka, NATS, LocalStack)
- GitHub Actions workflows for CI (build, test, lint) and benchmarks
- MIT License

[0.1.0]: https://github.com/eventmesh/eventmesh/releases/tag/v0.1.0
