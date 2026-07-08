using EventMesh.Abstractions.Configuration;

namespace EventMesh.Abstractions.Plugins;

/// <summary>
/// Extension point for registering EventMesh plugins.
/// </summary>
public interface IEventMeshPlugin
{
    /// <summary>
    /// Gets the plugin name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the plugin version.
    /// </summary>
    Version Version { get; }

    /// <summary>
    /// Configures the plugin against EventMesh options.
    /// </summary>
    /// <param name="options">The EventMesh configuration to modify.</param>
    void Configure(EventMeshOptions options);
}
