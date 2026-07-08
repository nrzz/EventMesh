namespace EventMesh.Abstractions.Transport;

/// <summary>
/// A message as it is sent or received at the transport layer.
/// </summary>
public sealed class TransportMessage
{
    /// <summary>
    /// Gets or sets the transport-assigned or application message identifier.
    /// </summary>
    public string? MessageId { get; set; }

    /// <summary>
    /// Gets or sets the destination topic, queue, or subject.
    /// </summary>
    public required string Destination { get; set; }

    /// <summary>
    /// Gets or sets the routing key or binding key.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets or sets the serialized message body.
    /// </summary>
    public ReadOnlyMemory<byte> Body { get; set; }

    /// <summary>
    /// Gets or sets the content type of the message body.
    /// </summary>
    public string? ContentType { get; set; }

    /// <summary>
    /// Gets or sets transport headers or user properties.
    /// </summary>
    public IDictionary<string, string> Headers { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets the correlation identifier.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the reply-to address for request/response scenarios.
    /// </summary>
    public string? ReplyTo { get; set; }

    /// <summary>
    /// Gets or sets the message priority.
    /// </summary>
    public int? Priority { get; set; }

    /// <summary>
    /// Gets or sets the time-to-live for the message.
    /// </summary>
    public TimeSpan? TimeToLive { get; set; }

    /// <summary>
    /// Gets or sets the scheduled delivery time.
    /// </summary>
    public DateTimeOffset? ScheduledAt { get; set; }

    /// <summary>
    /// Gets or sets the session identifier.
    /// </summary>
    public string? SessionId { get; set; }

    /// <summary>
    /// Gets or sets the partition key.
    /// </summary>
    public string? PartitionKey { get; set; }

    /// <summary>
    /// Gets or sets the delivery tag or receipt handle assigned by the broker on receive.
    /// </summary>
    public string? DeliveryTag { get; set; }

    /// <summary>
    /// Gets or sets the number of times the message has been redelivered.
    /// </summary>
    public int DeliveryCount { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when the message was enqueued by the broker.
    /// </summary>
    public DateTimeOffset? EnqueuedAt { get; set; }
}
