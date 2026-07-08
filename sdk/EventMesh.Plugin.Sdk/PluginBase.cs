using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Plugins;

namespace EventMesh.Plugin.Sdk;

/// <summary>
/// Base class for EventMesh plugins with versioned manifest metadata.
/// </summary>
public abstract class PluginBase : IEventMeshPlugin
{
    /// <summary>
    /// Gets the plugin manifest describing identity and compatibility.
    /// </summary>
    public abstract PluginManifest Manifest { get; }

    /// <inheritdoc />
    public string Name => Manifest.Name;

    /// <inheritdoc />
    public Version Version => Manifest.Version;

    /// <inheritdoc />
    public virtual void Configure(EventMeshOptions options)
    {
    }

    /// <summary>
    /// Configures plugin services and pipeline filters through the plugin builder.
    /// </summary>
    /// <param name="builder">The plugin builder supplied by the host.</param>
    public virtual void ConfigurePlugin(IPluginBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        Configure(builder.Options);
    }
}
