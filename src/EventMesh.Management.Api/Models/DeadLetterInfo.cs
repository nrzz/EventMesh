namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes a dead-lettered message.
/// </summary>
public sealed class DeadLetterInfo
{
    /// <summary>
    /// Gets or sets the dead-letter record identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets or sets the original message identifier.
    /// </summary>
    public required string MessageId { get; init; }

    /// <summary>
    /// Gets or sets the message type.
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Gets or sets the original destination.
    /// </summary>
    public required string OriginalDestination { get; init; }

    /// <summary>
    /// Gets or sets the dead-letter queue or topic.
    /// </summary>
    public required string DeadLetterDestination { get; init; }

    /// <summary>
    /// Gets or sets the transport name.
    /// </summary>
    public required string Transport { get; init; }

    /// <summary>
    /// Gets or sets the failure reason.
    /// </summary>
    public string? FailureReason { get; init; }

    /// <summary>
    /// Gets or sets the number of delivery attempts before dead-lettering.
    /// </summary>
    public int DeliveryAttempts { get; init; }

    /// <summary>
    /// Gets or sets when the message was dead-lettered.
    /// </summary>
    public DateTimeOffset DeadLetteredAt { get; init; }
}

/// <summary>
/// Request to reprocess a dead-lettered message.
/// </summary>
public sealed class ReprocessDeadLetterRequest
{
    /// <summary>
    /// Gets or sets the destination to publish the message to.
    /// </summary>
    public string? Destination { get; init; }
}
