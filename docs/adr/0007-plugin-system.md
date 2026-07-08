# ADR-0007: Plugin System

## Status

Accepted

## Date

2026-07-08

## Context

EventMesh must support extensibility beyond the seven built-in transports. Organizations need custom serialization formats, compression algorithms, encryption providers, authentication mechanisms, metrics exporters, and dashboard widgets without forking the core framework.

The plugin system must be:

- Safe to load in production (no arbitrary code execution risks beyond normal .NET assembly loading)
- Version-compatible with the host EventMesh version
- Discoverable at runtime for the dashboard and CLI
- Simple to author and distribute as NuGet packages

## Decision

Plugins are .NET assemblies distributed as NuGet packages, implementing `IEventMeshPlugin` with a semver-versioned manifest.

### Plugin contract

```csharp
public interface IEventMeshPlugin
{
    PluginManifest Manifest { get; }
    void Configure(IServiceCollection services, IFilterRegistration filters);
}
```

```csharp
public record PluginManifest(
    string Id,
    string Name,
    Version Version,
    Version MinHostVersion,
    IReadOnlyList<string> Capabilities);
```

### Registration (application host)

Plugins are registered explicitly via DI:

```csharp
builder.Services
    .AddEventMesh()
    .AddPlugin<GzipCompressionPlugin>()
    .AddPlugin<AesGcmEncryptionPlugin>()
    .AddPlugin<VaultSecretProviderPlugin>();
```

Explicit registration ensures only trusted plugins are loaded in production.

### Discovery (dashboard and CLI)

The management API and CLI scan a configured plugins directory using `AssemblyLoadContext`:

```json
{
  "EventMesh": {
    "Plugins": {
      "Directory": "/opt/eventmesh/plugins",
      "AutoLoad": true
    }
  }
}
```

Each plugin assembly contains a `eventmesh.plugin.json` manifest for metadata display without loading the full assembly.

### Plugin categories

| Category | Interface | Example |
|----------|-----------|---------|
| Serialization | `IMessageSerializer` | Avro, Protobuf, MessagePack |
| Compression | `IMessageFilter` (compress) | gzip, zstd |
| Encryption | `IMessageFilter` (encrypt) | AES-GCM |
| Authentication | `IAuthenticationProvider` | OAuth2, API key |
| Secret provider | `ISecretProvider` | Vault, AWS SM, Azure KV |
| Metrics exporter | `IMetricsExporter` | Datadog, New Relic |
| Tracing exporter | `ITracingExporter` | Jaeger, Zipkin |
| Dashboard widget | `IDashboardWidget` | Custom queue monitor |

### Versioning

- Plugins declare `MinHostVersion` in their manifest
- At load time, EventMesh compares plugin version against host version
- Incompatible plugins are rejected with a clear error message
- Plugin `Id` is stable across versions; `Version` follows semver

### First-party plugins (Milestone 13)

| Plugin | Package |
|--------|---------|
| gzip/zstd compression | `EventMesh.Plugins.Compression` |
| AES-GCM encryption | `EventMesh.Plugins.Encryption` |
| Vault/AWS/Azure secrets | `EventMesh.Plugins.Secrets` |
| Datadog/New Relic metrics | `EventMesh.Plugins.Exporters` |

### SDK

`EventMesh.Plugin.Sdk` provides:

- Base classes for common plugin types
- Manifest schema validation
- Roslyn analyzer for plugin authoring conventions
- Template project for `dotnet new eventmesh-plugin`

## Consequences

### Positive

- Extensibility without forking core
- NuGet distribution aligns with .NET ecosystem conventions
- Explicit registration prevents surprise plugin loading in production
- Version compatibility checks prevent runtime failures

### Negative

- `AssemblyLoadContext` scanning adds complexity and potential assembly conflict issues
- Plugin quality varies; poorly written plugins can affect pipeline performance
- Security review is the deployer's responsibility for third-party plugins

### Neutral

- First-party plugins serve as reference implementations
- Plugin marketplace/registry is a future consideration (post-GA)

## References

- [ADR-0003: Pipeline Architecture](0003-pipeline-architecture.md)
- [EventMesh.Plugin.Sdk](../../sdk/EventMesh.Plugin.Sdk/)
