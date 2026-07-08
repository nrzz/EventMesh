using EventMesh.Abstractions.Envelope;

namespace EventMesh.Abstractions.Reliability;

/// <summary>
/// Represents a message stored in the transactional outbox.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>
    /// Gets or sets the unique outbox record identifier.
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    /// Gets or sets the CloudEvents envelope to publish.
    /// </summary>
    public required MessageEnvelope Envelope { get; set; }

    /// <summary>
    /// Gets or sets the destination topic or queue.
    /// </summary>
    public required string Destination { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when the message was written to the outbox.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets or sets the UTC timestamp when the message was successfully published.
    /// </summary>
    public DateTimeOffset? PublishedAt { get; set; }

    /// <summary>
    /// Gets or sets the number of publish attempts made for this message.
    /// </summary>
    public int AttemptCount { get; set; }

    /// <summary>
    /// Gets or sets the processing state of the outbox message.
    /// </summary>
    public OutboxMessageState State { get; set; } = OutboxMessageState.Pending;

    /// <summary>
    /// Gets or sets the last error message when publishing failed.
    /// </summary>
    public string? LastError { get; set; }

    /// <summary>
    /// Gets or sets the idempotency key associated with the message.
    /// </summary>
    public string? IdempotencyKey { get; set; }
}

/// <summary>
/// Processing states for outbox messages.
/// </summary>
public enum OutboxMessageState
{
    /// <summary>
    /// The message is waiting to be published.
    /// </summary>
    Pending = 0,

    /// <summary>
    /// The message is currently being published.
    /// </summary>
    InProgress = 1,

    /// <summary>
    /// The message was published successfully.
    /// </summary>
    Published = 2,

    /// <summary>
    /// The message failed and will not be retried automatically.
    /// </summary>
    Failed = 3,
}
