using System.Text.Json;
using EventMesh.Abstractions.Serialization;

namespace EventMesh.Core.Serialization;

/// <summary>
/// JSON message serializer using <see cref="System.Text.Json"/>.
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions DefaultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    /// <inheritdoc />
    public SerializationFormat Format => SerializationFormat.Json;

    /// <inheritdoc />
    public string DefaultContentType => "application/json";

    /// <inheritdoc />
    public ValueTask<ReadOnlyMemory<byte>> SerializeAsync<T>(
        T message,
        string? contentType = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        cancellationToken.ThrowIfCancellationRequested();
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message, DefaultOptions);
        return ValueTask.FromResult<ReadOnlyMemory<byte>>(bytes);
    }

    /// <inheritdoc />
    public ValueTask<T> DeserializeAsync<T>(
        ReadOnlyMemory<byte> data,
        string? contentType = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        cancellationToken.ThrowIfCancellationRequested();
        var message = JsonSerializer.Deserialize<T>(data.Span, DefaultOptions)
            ?? throw new InvalidOperationException($"Failed to deserialize message to type '{typeof(T).FullName}'.");
        return ValueTask.FromResult(message);
    }
}
