namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes a message observed in the mesh.
/// </summary>
public sealed class MessageInfo
{
    /// <summary>
    /// Gets or sets the message identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets or sets the message type.
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Gets or sets the source topic or queue.
    /// </summary>
    public required string Source { get; init; }

    /// <summary>
    /// Gets or sets the transport name.
    /// </summary>
    public required string Transport { get; init; }

    /// <summary>
    /// Gets or sets the correlation identifier.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets or sets when the message was published.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets or sets the message size in bytes.
    /// </summary>
    public int SizeBytes { get; init; }

    /// <summary>
    /// Gets or sets the message delivery status.
    /// </summary>
    public string Status { get; init; } = "delivered";

    /// <summary>
    /// Gets or sets a preview of the message payload.
    /// </summary>
    public string? PayloadPreview { get; init; }

    /// <summary>
    /// Gets or sets message headers.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
