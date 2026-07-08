namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes a messaging queue.
/// </summary>
public sealed class QueueInfo
{
    /// <summary>
    /// Gets or sets the queue name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the transport hosting the queue.
    /// </summary>
    public required string Transport { get; init; }

    /// <summary>
    /// Gets or sets the current depth (ready messages).
    /// </summary>
    public long Depth { get; init; }

    /// <summary>
    /// Gets or sets the number of messages currently in-flight.
    /// </summary>
    public long InFlight { get; init; }

    /// <summary>
    /// Gets or sets the consumer count attached to the queue.
    /// </summary>
    public int ConsumerCount { get; init; }

    /// <summary>
    /// Gets or sets whether the queue is durable.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// Gets or sets the dead-letter destination if configured.
    /// </summary>
    public string? DeadLetterDestination { get; init; }
}

/// <summary>
/// Request to create or update a queue.
/// </summary>
public sealed class CreateQueueRequest
{
    /// <summary>
    /// Gets or sets the queue name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the target transport.
    /// </summary>
    public string? Transport { get; init; }

    /// <summary>
    /// Gets or sets whether the queue should be durable.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// Gets or sets the dead-letter destination.
    /// </summary>
    public string? DeadLetterDestination { get; init; }
}

/// <summary>
/// Request to purge all messages from a queue.
/// </summary>
public sealed class PurgeQueueRequest
{
    /// <summary>
    /// Gets or sets the queue name to purge.
    /// </summary>
    public required string Name { get; init; }
}
