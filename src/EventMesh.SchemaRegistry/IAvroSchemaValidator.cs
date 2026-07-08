using Avro;

namespace EventMesh.SchemaRegistry;

/// <summary>
/// Validates Avro schema definitions.
/// </summary>
public interface IAvroSchemaValidator
{
    /// <summary>
    /// Validates the supplied Avro schema definition.
    /// </summary>
    void Validate(string definition);
}

/// <summary>
/// Default Avro schema validator backed by Apache.Avro.
/// </summary>
public sealed class AvroSchemaValidator : IAvroSchemaValidator
{
    /// <inheritdoc />
    public void Validate(string definition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(definition);
        Schema.Parse(definition);
    }
}
