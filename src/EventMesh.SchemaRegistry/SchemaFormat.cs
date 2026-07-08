namespace EventMesh.SchemaRegistry;

/// <summary>
/// Supported schema formats in the registry.
/// </summary>
public enum SchemaFormat
{
    /// <summary>
    /// JSON Schema.
    /// </summary>
    Json,

    /// <summary>
    /// Apache Avro schema.
    /// </summary>
    Avro,
}
