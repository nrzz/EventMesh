using System.Collections.Concurrent;

namespace EventMesh.SchemaRegistry;

/// <summary>
/// In-memory implementation of <see cref="ISchemaRegistry"/>.
/// </summary>
public sealed class InMemorySchemaRegistry : ISchemaRegistry
{
    private readonly ConcurrentDictionary<string, SortedDictionary<int, SchemaRegistration>> _schemas = new(
        StringComparer.OrdinalIgnoreCase);

    private readonly IJsonSchemaValidator _jsonValidator;
    private readonly IAvroSchemaValidator _avroValidator;

    public InMemorySchemaRegistry(IJsonSchemaValidator jsonValidator, IAvroSchemaValidator avroValidator)
    {
        _jsonValidator = jsonValidator;
        _avroValidator = avroValidator;
    }

    /// <inheritdoc />
    public Task<SchemaRegistration> RegisterAsync(
        string subject,
        SchemaFormat format,
        string definition,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentException.ThrowIfNullOrWhiteSpace(definition);

        ValidateDefinition(format, definition);

        var versions = _schemas.GetOrAdd(subject, _ => new SortedDictionary<int, SchemaRegistration>());
        var version = versions.Count == 0 ? 1 : versions.Keys.Max() + 1;
        var registration = new SchemaRegistration
        {
            Subject = subject,
            Version = version,
            Format = format,
            Definition = definition,
            RegisteredAt = DateTimeOffset.UtcNow,
        };

        versions[version] = registration;
        return Task.FromResult(registration);
    }

    /// <inheritdoc />
    public Task<SchemaRegistration?> GetAsync(
        string subject,
        int version,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        if (!_schemas.TryGetValue(subject, out var versions) || !versions.TryGetValue(version, out var registration))
        {
            return Task.FromResult<SchemaRegistration?>(null);
        }

        return Task.FromResult<SchemaRegistration?>(registration);
    }

    /// <inheritdoc />
    public Task<SchemaRegistration?> GetLatestAsync(
        string subject,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        if (!_schemas.TryGetValue(subject, out var versions) || versions.Count == 0)
        {
            return Task.FromResult<SchemaRegistration?>(null);
        }

        var latest = versions[versions.Keys.Max()];
        return Task.FromResult<SchemaRegistration?>(latest);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListSubjectsAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> subjects = _schemas.Keys.OrderBy(static subject => subject, StringComparer.OrdinalIgnoreCase).ToArray();
        return Task.FromResult(subjects);
    }

    private void ValidateDefinition(SchemaFormat format, string definition)
    {
        switch (format)
        {
            case SchemaFormat.Json:
                _jsonValidator.Validate(definition);
                break;
            case SchemaFormat.Avro:
                _avroValidator.Validate(definition);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported schema format.");
        }
    }
}
