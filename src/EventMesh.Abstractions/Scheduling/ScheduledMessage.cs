using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Messaging;

namespace EventMesh.Abstractions.Scheduling;

/// <summary>
/// Represents a message scheduled for future delivery.
/// </summary>
public sealed class ScheduledMessage
{
    /// <summary>
    /// Gets or sets the unique schedule identifier.
    /// </summary>
    public required string ScheduleId { get; set; }

    /// <summary>
    /// Gets or sets the CloudEvents envelope to deliver.
    /// </summary>
    public required MessageEnvelope Envelope { get; set; }

    /// <summary>
    /// Gets or sets the destination topic or queue.
    /// </summary>
    public required string Destination { get; set; }

    /// <summary>
    /// Gets or sets the UTC time when the message should be delivered.
    /// </summary>
    public DateTimeOffset ScheduledAt { get; set; }

    /// <summary>
    /// Gets or sets the UTC time when the schedule was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets or sets the optional schedule group identifier.
    /// </summary>
    public string? ScheduleGroupId { get; set; }

    /// <summary>
    /// Gets or sets the schedule options used when the message was created.
    /// </summary>
    public ScheduleOptions? Options { get; set; }

    /// <summary>
    /// Gets or sets the current state of the scheduled message.
    /// </summary>
    public ScheduledMessageState State { get; set; } = ScheduledMessageState.Pending;

    /// <summary>
    /// Gets or sets the UTC time when the message was delivered or cancelled.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; set; }
}

/// <summary>
/// Lifecycle states for scheduled messages.
/// </summary>
public enum ScheduledMessageState
{
    /// <summary>
    /// The message is waiting to be delivered.
    /// </summary>
    Pending = 0,

    /// <summary>
    /// The message is currently being dispatched.
    /// </summary>
    Dispatching = 1,

    /// <summary>
    /// The message was delivered successfully.
    /// </summary>
    Delivered = 2,

    /// <summary>
    /// The schedule was cancelled before delivery.
    /// </summary>
    Cancelled = 3,

    /// <summary>
    /// Delivery failed and will not be retried automatically.
    /// </summary>
    Failed = 4,
}
