using EventMesh.Abstractions.Envelope;

namespace EventMesh.Abstractions.Reliability;

/// <summary>
/// Represents a message recorded in the idempotent inbox.
/// </summary>
public sealed class InboxMessage
{
    /// <summary>
    /// Gets or sets the unique inbox record identifier.
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    /// Gets or sets the idempotency key used to deduplicate processing.
    /// </summary>
    public required string IdempotencyKey { get; set; }

    /// <summary>
    /// Gets or sets the CloudEvents envelope that was received.
    /// </summary>
    public required MessageEnvelope Envelope { get; set; }

    /// <summary>
    /// Gets or sets the consumer or handler that processed the message.
    /// </summary>
    public string? ConsumerId { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when the message was first received.
    /// </summary>
    public DateTimeOffset ReceivedAt { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets or sets the UTC timestamp when processing completed successfully.
    /// </summary>
    public DateTimeOffset? ProcessedAt { get; set; }

    /// <summary>
    /// Gets or sets the processing state of the inbox message.
    /// </summary>
    public InboxMessageState State { get; set; } = InboxMessageState.Received;

    /// <summary>
    /// Gets or sets the last error message when processing failed.
    /// </summary>
    public string? LastError { get; set; }
}

/// <summary>
/// Processing states for inbox messages.
/// </summary>
public enum InboxMessageState
{
    /// <summary>
    /// The message was received but not yet processed.
    /// </summary>
    Received = 0,

    /// <summary>
    /// The message is currently being processed.
    /// </summary>
    Processing = 1,

    /// <summary>
    /// The message was processed successfully.
    /// </summary>
    Processed = 2,

    /// <summary>
    /// The message was a duplicate of an already processed message.
    /// </summary>
    Duplicate = 3,

    /// <summary>
    /// Processing failed and will not be retried automatically.
    /// </summary>
    Failed = 4,
}
