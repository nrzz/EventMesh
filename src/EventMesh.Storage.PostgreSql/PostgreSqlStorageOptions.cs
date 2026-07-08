namespace EventMesh.Storage.PostgreSql;

/// <summary>
/// Configuration options for PostgreSQL-backed EventMesh storage.
/// </summary>
public sealed class PostgreSqlStorageOptions
{
    /// <summary>
    /// Gets or sets the PostgreSQL connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets a value indicating whether storage tables are created automatically on first use.
    /// </summary>
    public bool AutoInitializeSchema { get; set; } = true;
}
