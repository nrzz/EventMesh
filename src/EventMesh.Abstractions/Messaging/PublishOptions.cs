namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// Options that control how a message is published to the mesh.
/// </summary>
public sealed class PublishOptions
{
    /// <summary>
    /// Gets or sets the destination topic or exchange name.
    /// </summary>
    public string? Topic { get; set; }

    /// <summary>
    /// Gets or sets the routing key used by transports that support topic routing.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets or sets the message priority when supported by the underlying broker.
    /// </summary>
    public int? Priority { get; set; }

    /// <summary>
    /// Gets or sets the time-to-live for the published message.
    /// </summary>
    public TimeSpan? TimeToLive { get; set; }

    /// <summary>
    /// Gets or sets the correlation identifier propagated with the message.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the causation identifier that links this message to its trigger.
    /// </summary>
    public string? CausationId { get; set; }

    /// <summary>
    /// Gets or sets the CloudEvents subject for the message.
    /// </summary>
    public string? Subject { get; set; }

    /// <summary>
    /// Gets or sets the CloudEvents source URI for the message.
    /// </summary>
    public string? Source { get; set; }

    /// <summary>
    /// Gets or sets the explicit CloudEvents type override for the message.
    /// </summary>
    public string? MessageType { get; set; }

    /// <summary>
    /// Gets or sets transport-specific or application headers attached to the message.
    /// </summary>
    public IDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Gets or sets the content type of the serialized payload.
    /// </summary>
    public string? ContentType { get; set; }

    /// <summary>
    /// Gets or sets the session identifier for transports that support sessions.
    /// </summary>
    public string? SessionId { get; set; }

    /// <summary>
    /// Gets or sets the partition key for transports that support partitioning.
    /// </summary>
    public string? PartitionKey { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether publisher confirms are required.
    /// </summary>
    public bool RequirePublisherConfirm { get; set; }

    /// <summary>
    /// Gets or sets the timeout used when waiting for publisher confirmation.
    /// </summary>
    public TimeSpan? PublisherConfirmTimeout { get; set; }

    /// <summary>
    /// Creates a shallow copy of the current options instance.
    /// </summary>
    public PublishOptions Clone() => new()
    {
        Topic = Topic,
        RoutingKey = RoutingKey,
        Priority = Priority,
        TimeToLive = TimeToLive,
        CorrelationId = CorrelationId,
        CausationId = CausationId,
        Subject = Subject,
        Source = Source,
        MessageType = MessageType,
        Headers = Headers is null ? null : new Dictionary<string, string>(Headers),
        ContentType = ContentType,
        SessionId = SessionId,
        PartitionKey = PartitionKey,
        RequirePublisherConfirm = RequirePublisherConfirm,
        PublisherConfirmTimeout = PublisherConfirmTimeout,
    };
}
