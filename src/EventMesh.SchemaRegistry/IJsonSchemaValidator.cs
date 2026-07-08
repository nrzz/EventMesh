using System.Text.Json;

namespace EventMesh.SchemaRegistry;

/// <summary>
/// Validates JSON Schema definitions.
/// </summary>
public interface IJsonSchemaValidator
{
    /// <summary>
    /// Validates the supplied JSON Schema definition.
    /// </summary>
    void Validate(string definition);
}

/// <summary>
/// Default JSON Schema validator that ensures the definition is valid JSON with a schema type.
/// </summary>
public sealed class JsonSchemaValidator : IJsonSchemaValidator
{
    /// <inheritdoc />
    public void Validate(string definition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(definition);

        using var document = JsonDocument.Parse(definition);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("JSON Schema definitions must be JSON objects.");
        }

        if (!document.RootElement.TryGetProperty("$schema", out _) &&
            !document.RootElement.TryGetProperty("type", out _))
        {
            throw new InvalidOperationException("JSON Schema definitions must include '$schema' or 'type'.");
        }
    }
}
