# Contributing to EventMesh

Thank you for your interest in contributing to EventMesh. This guide covers the development workflow, coding standards, and pull request process.

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md). Be respectful, inclusive, and constructive in all interactions.

## Getting Started

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (for integration tests and local brokers)
- [Git](https://git-scm.com/)

### Clone and build

```bash
git clone https://github.com/eventmesh/eventmesh.git
cd eventmesh
docker compose -f docker/docker-compose.yml up -d
dotnet restore EventMesh.slnx
dotnet build EventMesh.slnx --configuration Release
dotnet test EventMesh.slnx --configuration Release
```

### Project structure

| Directory | Purpose |
|-----------|---------|
| `src/EventMesh.Abstractions/` | Zero-dependency public contracts |
| `src/EventMesh.Core/` | Runtime engine and DI extensions |
| `src/EventMesh.Transport.*/` | Broker adapter packages |
| `src/EventMesh.Storage.*/` | Persistence implementations |
| `tests/` | Unit, integration, and compatibility tests |
| `benchmarks/` | BenchmarkDotNet performance suite |
| `docs/adr/` | Architecture Decision Records |
| `docker/` | Local development infrastructure |

## Development Workflow

1. **Check the roadmap** — Review [ROADMAP.md](ROADMAP.md) to see active milestones and avoid duplicate work
2. **Open an issue** — For significant changes, open a GitHub issue to discuss the approach before coding
3. **Create a branch** — Branch from `main` using the naming convention below
4. **Implement** — Write code, tests, and documentation together
5. **Verify locally** — Build, test, and format before pushing
6. **Open a pull request** — Fill out the PR template and request review

### Branch naming

```
feature/<short-description>    # New features
fix/<short-description>         # Bug fixes
docs/<short-description>        # Documentation only
refactor/<short-description>    # Refactoring without behavior change
test/<short-description>        # Test additions or fixes
```

### Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(core): add retry filter with exponential backoff
fix(rabbitmq): correct publisher confirm timeout handling
docs(adr): add ADR-0009 for saga orchestration
test(kafka): add offset replay compatibility tests
chore(ci): update BenchmarkDotNet to 0.14.0
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `ci`, `build`.

## Pull Request Process

1. **One concern per PR** — Keep pull requests focused. Large changes should be split into reviewable increments aligned with milestones.
2. **Tests required** — All new behavior must include unit tests. Transport changes must pass the shared compatibility suite in `tests/EventMesh.Transport.Compatibility.Tests/`.
3. **Documentation** — Update relevant docs (README, ARCHITECTURE, ADRs, broker guides) when behavior or public API changes.
4. **No placeholders** — Do not submit code with `TODO` comments, stub methods, or `NotImplementedException` in production paths.
5. **Format check** — CI verifies formatting. Run locally before pushing:

   ```bash
   dotnet format EventMesh.slnx --verify-no-changes
   ```

6. **Build must pass** — `dotnet build` and `dotnet test` must succeed with warnings treated as errors (`TreatWarningsAsErrors` is enabled).
7. **Review** — At least one maintainer approval is required before merge.
8. **Squash merge** — PRs are squash-merged to `main` with the PR title as the commit message.

### PR checklist

- [ ] Builds without warnings on `net10.0`
- [ ] All tests pass locally
- [ ] New code has corresponding tests
- [ ] Transport adapters pass compatibility suite (if applicable)
- [ ] Public APIs have XML documentation comments
- [ ] CHANGELOG.md updated under `[Unreleased]` (if user-facing change)
- [ ] ADR created or updated for architectural decisions
- [ ] `dotnet format --verify-no-changes` passes

## Coding Standards

### C# conventions

- **Target framework:** `net10.0` (set in `Directory.Build.props`)
- **Nullable reference types:** enabled project-wide
- **Warnings as errors:** enabled; no suppressions without justification
- **Naming:** PascalCase for public members, `_camelCase` for private fields, `I` prefix for interfaces
- **Async:** suffix async methods with `Async`; accept `CancellationToken` on all I/O-bound public APIs
- **DI:** use constructor injection; register services via `IServiceCollection` extension methods in `*ServiceCollectionExtensions.cs`
- **Immutability:** prefer `record` types for DTOs, options, and envelope types

### Package boundaries

- `EventMesh.Abstractions` must have **zero NuGet dependencies**
- Transport packages depend on `Abstractions` and `Core` only — never on each other
- Control plane (`Management.Api`, CLI) depends on `Core` but `Core` must not depend on control plane packages

### Error handling

- Throw `CapabilityNotSupportedException` at configuration time for unsupported broker features
- Use specific exception types from `EventMesh.Abstractions` — avoid generic `Exception`
- Never swallow exceptions in filters; log and propagate or move to DLQ

### Testing conventions

- **Framework:** xUnit with FluentAssertions and Moq
- **Naming:** `MethodName_Scenario_ExpectedResult` (e.g., `PublishAsync_WhenOutboxEnabled_PersistsBeforeSending`)
- **Integration tests:** use Testcontainers; tag cloud-specific tests with `[Trait("Category", "Cloud")]` for opt-in CI runs
- **Compatibility tests:** shared suite in `EventMesh.Transport.Compatibility.Tests`; every transport must implement `ITransportTestHost`

### Documentation

- Public APIs require XML doc comments (`///`)
- Architectural decisions require an ADR in `docs/adr/` using the numbered format
- Broker-specific guides go in `docs/brokers/`

## Adding a New Transport Adapter

1. Create `src/EventMesh.Transport.<Broker>/` project referencing `Abstractions` and `Core`
2. Implement `IBrokerTransport` and `IBrokerTransportFactory`
3. Declare accurate `BrokerCapabilities` flags
4. Add Testcontainers-based integration tests
5. Implement `ITransportTestHost` in the compatibility test project
6. Pass the full compatibility suite
7. Add a row to [docs/broker-capability-matrix.md](docs/broker-capability-matrix.md)
8. Update README supported brokers table

## Running Benchmarks

```bash
dotnet run --project benchmarks/EventMesh.Benchmarks -c Release
```

Benchmark results are compared against baselines in CI. Significant regressions block merge.

## Reporting Issues

- **Bugs:** Use the GitHub bug report template with reproduction steps
- **Feature requests:** Use the feature request template; reference the roadmap milestone if applicable
- **Security vulnerabilities:** See [SECURITY.md](SECURITY.md) — do not file public issues

## License

By contributing to EventMesh, you agree that your contributions will be licensed under the [MIT License](LICENSE).
