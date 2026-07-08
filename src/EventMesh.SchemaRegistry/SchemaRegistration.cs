namespace EventMesh.SchemaRegistry;

/// <summary>
/// Represents a registered schema definition.
/// </summary>
public sealed class SchemaRegistration
{
    /// <summary>
    /// Gets or sets the subject name for the schema.
    /// </summary>
    public required string Subject { get; set; }

    /// <summary>
    /// Gets or sets the schema version number.
    /// </summary>
    public int Version { get; set; }

    /// <summary>
    /// Gets or sets the schema format.
    /// </summary>
    public SchemaFormat Format { get; set; }

    /// <summary>
    /// Gets or sets the schema definition text.
    /// </summary>
    public required string Definition { get; set; }

    /// <summary>
    /// Gets or sets when the schema was registered.
    /// </summary>
    public DateTimeOffset RegisteredAt { get; set; } = DateTimeOffset.UtcNow;
}
