namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// Options that control request/response messaging over the bus.
/// </summary>
public sealed class RequestOptions
{
    /// <summary>
    /// Gets or sets the destination topic or queue for the request message.
    /// </summary>
    public string? Topic { get; set; }

    /// <summary>
    /// Gets or sets the routing key used by transports that support topic routing.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets or sets the maximum time to wait for a response.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the correlation identifier for the request.
    /// When not set, one is generated automatically.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the causation identifier that links this request to its trigger.
    /// </summary>
    public string? CausationId { get; set; }

    /// <summary>
    /// Gets or sets transport-specific or application headers attached to the request.
    /// </summary>
    public IDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Gets or sets the content type of the serialized request payload.
    /// </summary>
    public string? ContentType { get; set; }

    /// <summary>
    /// Gets or sets the expected response content type.
    /// </summary>
    public string? ResponseContentType { get; set; }

    /// <summary>
    /// Gets or sets the session identifier for transports that support sessions.
    /// </summary>
    public string? SessionId { get; set; }

    /// <summary>
    /// Gets or sets the partition key for transports that support partitioning.
    /// </summary>
    public string? PartitionKey { get; set; }

    /// <summary>
    /// Gets or sets the reply-to address override for the response channel.
    /// </summary>
    public string? ReplyTo { get; set; }

    /// <summary>
    /// Creates a shallow copy of the current options instance.
    /// </summary>
    public RequestOptions Clone() => new()
    {
        Topic = Topic,
        RoutingKey = RoutingKey,
        Timeout = Timeout,
        CorrelationId = CorrelationId,
        CausationId = CausationId,
        Headers = Headers is null ? null : new Dictionary<string, string>(Headers),
        ContentType = ContentType,
        ResponseContentType = ResponseContentType,
        SessionId = SessionId,
        PartitionKey = PartitionKey,
        ReplyTo = ReplyTo,
    };
}
