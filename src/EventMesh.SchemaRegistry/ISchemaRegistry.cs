namespace EventMesh.SchemaRegistry;

/// <summary>
/// Registry for message schema definitions.
/// </summary>
public interface ISchemaRegistry
{
    /// <summary>
    /// Registers a schema for the specified subject.
    /// </summary>
    Task<SchemaRegistration> RegisterAsync(
        string subject,
        SchemaFormat format,
        string definition,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a schema by subject and version.
    /// </summary>
    Task<SchemaRegistration?> GetAsync(
        string subject,
        int version,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the latest schema version for a subject.
    /// </summary>
    Task<SchemaRegistration?> GetLatestAsync(
        string subject,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all registered subjects.
    /// </summary>
    Task<IReadOnlyList<string>> ListSubjectsAsync(CancellationToken cancellationToken = default);
}
