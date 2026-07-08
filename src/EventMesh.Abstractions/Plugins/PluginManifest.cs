namespace EventMesh.Abstractions.Plugins;

/// <summary>
/// Describes plugin metadata for discovery and compatibility checks.
/// </summary>
public sealed class PluginManifest
{
    /// <summary>
    /// Gets or sets the plugin name.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Gets or sets the plugin version.
    /// </summary>
    public required Version Version { get; set; }

    /// <summary>
    /// Gets or sets the plugin description.
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// Gets or sets the plugin author.
    /// </summary>
    public string? Author { get; set; }

    /// <summary>
    /// Gets or sets the minimum EventMesh version required by the plugin.
    /// </summary>
    public Version? MinimumEventMeshVersion { get; set; }

    /// <summary>
    /// Gets or sets the assembly name containing the plugin implementation.
    /// </summary>
    public string? AssemblyName { get; set; }

    /// <summary>
    /// Gets or sets the fully qualified type name of the plugin entry point.
    /// </summary>
    public string? EntryPointType { get; set; }

    /// <summary>
    /// Gets or sets plugin tags used for discovery and filtering.
    /// </summary>
    public IList<string> Tags { get; set; } = new List<string>();

    /// <summary>
    /// Gets or sets plugin-specific configuration values.
    /// </summary>
    public IDictionary<string, string> Properties { get; set; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Determines whether the plugin is compatible with the specified EventMesh version.
    /// </summary>
    public bool IsCompatibleWith(Version eventMeshVersion)
    {
        ArgumentNullException.ThrowIfNull(eventMeshVersion);

        return MinimumEventMeshVersion is null || eventMeshVersion >= MinimumEventMeshVersion;
    }
}
