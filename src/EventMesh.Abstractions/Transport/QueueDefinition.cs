namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Describes a point-to-point queue in broker topology.
/// </summary>
public sealed class QueueDefinition
{
    /// <summary>
    /// Gets or sets the queue name.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue is durable.
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether the queue is exclusive to a single connection.
    /// </summary>
    public bool Exclusive { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue is automatically deleted when unused.
    /// </summary>
    public bool AutoDelete { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of messages the queue can hold.
    /// </summary>
    public long? MaxLength { get; set; }

    /// <summary>
    /// Gets or sets the maximum size in bytes the queue can hold.
    /// </summary>
    public long? MaxSizeBytes { get; set; }

    /// <summary>
    /// Gets or sets the default message time-to-live for the queue.
    /// </summary>
    public TimeSpan? DefaultTimeToLive { get; set; }

    /// <summary>
    /// Gets or sets the dead-letter destination for rejected messages.
    /// </summary>
    public string? DeadLetterDestination { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue enforces FIFO ordering.
    /// </summary>
    public bool Fifo { get; set; }

    /// <summary>
    /// Gets or sets transport-specific arguments for queue creation.
    /// </summary>
    public IDictionary<string, string> Arguments { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
