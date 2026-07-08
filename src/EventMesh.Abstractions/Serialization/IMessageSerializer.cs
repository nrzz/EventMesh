namespace EventMesh.Abstractions.Serialization;

/// <summary>
/// Serializes and deserializes message payloads for transport over the mesh.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Gets the serialization format implemented by this serializer.
    /// </summary>
    SerializationFormat Format { get; }

    /// <summary>
    /// Gets the default content type produced by this serializer.
    /// </summary>
    string DefaultContentType { get; }

    /// <summary>
    /// Serializes a message payload to bytes.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="message">The message to serialize.</param>
    /// <param name="contentType">The content type to use for serialization.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    ValueTask<ReadOnlyMemory<byte>> SerializeAsync<T>(
        T message,
        string? contentType = null,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Deserializes a message payload from bytes.
    /// </summary>
    /// <typeparam name="T">The expected message payload type.</typeparam>
    /// <param name="data">The serialized payload.</param>
    /// <param name="contentType">The content type of the payload.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    ValueTask<T> DeserializeAsync<T>(
        ReadOnlyMemory<byte> data,
        string? contentType = null,
        CancellationToken cancellationToken = default) where T : notnull;
}
