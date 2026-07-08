namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes an installed or discovered EventMesh plugin.
/// </summary>
public sealed class PluginInfo
{
    /// <summary>
    /// Gets or sets the plugin name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the plugin version.
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Gets or sets the plugin description.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Gets or sets the plugin author.
    /// </summary>
    public string? Author { get; init; }

    /// <summary>
    /// Gets or sets whether the plugin is enabled.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Gets or sets plugin tags.
    /// </summary>
    public IReadOnlyList<string> Tags { get; init; } = [];

    /// <summary>
    /// Gets or sets the plugin status.
    /// </summary>
    public string Status { get; init; } = "loaded";
}
