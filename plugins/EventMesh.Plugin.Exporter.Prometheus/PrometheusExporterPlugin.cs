using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Plugins;
using EventMesh.Plugin.Sdk;

namespace EventMesh.Plugin.Exporter.Prometheus;

/// <summary>
/// Prometheus metrics exporter plugin for EventMesh observability.
/// </summary>
public sealed class PrometheusExporterPlugin : PluginBase
{
    private static readonly Version PluginVersion = new(1, 0, 0);
    private static readonly Version MinHostVersion = new(0, 1, 0);

    /// <inheritdoc />
    public override PluginManifest Manifest { get; } = new()
    {
        Name = "prometheus-exporter",
        Version = PluginVersion,
        Description = "Exports EventMesh metrics via Prometheus scrape endpoints.",
        Author = "EventMesh",
        MinimumEventMeshVersion = MinHostVersion,
        AssemblyName = typeof(PrometheusExporterPlugin).Assembly.GetName().Name,
        EntryPointType = typeof(PrometheusExporterPlugin).FullName,
        Tags = ["observability", "metrics", "prometheus"],
        Properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["metricsPath"] = "/metrics",
        },
    };

    /// <inheritdoc />
    public override void Configure(EventMeshOptions options)
    {
        options.EnableOpenTelemetry = true;
        options.PluginSettings.TryAdd(Name, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["metricsPath"] = "/metrics",
            ["exporter"] = "prometheus",
        });
    }

    /// <inheritdoc />
    public override void ConfigurePlugin(IPluginBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        base.ConfigurePlugin(builder);
        builder.AddSingleton(this);
    }
}
