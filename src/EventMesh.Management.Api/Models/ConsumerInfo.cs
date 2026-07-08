namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes an active message consumer.
/// </summary>
public sealed class ConsumerInfo
{
    /// <summary>
    /// Gets or sets the consumer identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets or sets the consumer name or handler type.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the subscribed destination.
    /// </summary>
    public required string Destination { get; init; }

    /// <summary>
    /// Gets or sets the transport name.
    /// </summary>
    public required string Transport { get; init; }

    /// <summary>
    /// Gets or sets the consumer status.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Gets or sets the prefetch or concurrency count.
    /// </summary>
    public int Concurrency { get; init; } = 1;

    /// <summary>
    /// Gets or sets messages processed since startup.
    /// </summary>
    public long MessagesProcessed { get; init; }

    /// <summary>
    /// Gets or sets when the consumer started.
    /// </summary>
    public DateTimeOffset StartedAt { get; init; }

    /// <summary>
    /// Gets or sets when the consumer last processed a message.
    /// </summary>
    public DateTimeOffset? LastMessageAt { get; init; }
}
