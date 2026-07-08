namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// Options that control message subscription and consumer behavior.
/// </summary>
public sealed class SubscribeOptions
{
    /// <summary>
    /// Gets or sets the topic, queue, or subscription name to consume from.
    /// </summary>
    public string? Topic { get; set; }

    /// <summary>
    /// Gets or sets the routing key or binding pattern for topic-based subscriptions.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets or sets the consumer group name for transports that support consumer groups.
    /// </summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// Gets or sets the subscription name for durable subscriptions.
    /// </summary>
    public string? SubscriptionName { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of unacknowledged messages per consumer.
    /// </summary>
    public int? PrefetchCount { get; set; }

    /// <summary>
    /// Gets or sets the visibility timeout for in-flight messages.
    /// </summary>
    public TimeSpan? VisibilityTimeout { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the subscription starts from the earliest available offset.
    /// </summary>
    public bool StartFromBeginning { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the consumer should automatically acknowledge messages after successful handling.
    /// </summary>
    public bool AutoAcknowledge { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum degree of parallelism for message handling.
    /// </summary>
    public int MaxConcurrency { get; set; } = 1;

    /// <summary>
    /// Gets or sets transport-specific or application headers applied to the subscription.
    /// </summary>
    public IDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Gets or sets the dead-letter options used when message handling fails.
    /// </summary>
    public Reliability.DeadLetterOptions? DeadLetter { get; set; }

    /// <summary>
    /// Gets or sets the retry policy applied when message handling fails.
    /// </summary>
    public Reliability.RetryPolicy? RetryPolicy { get; set; }

    /// <summary>
    /// Creates a shallow copy of the current options instance.
    /// </summary>
    public SubscribeOptions Clone() => new()
    {
        Topic = Topic,
        RoutingKey = RoutingKey,
        ConsumerGroup = ConsumerGroup,
        SubscriptionName = SubscriptionName,
        PrefetchCount = PrefetchCount,
        VisibilityTimeout = VisibilityTimeout,
        StartFromBeginning = StartFromBeginning,
        AutoAcknowledge = AutoAcknowledge,
        MaxConcurrency = MaxConcurrency,
        Headers = Headers is null ? null : new Dictionary<string, string>(Headers),
        DeadLetter = DeadLetter?.Clone(),
        RetryPolicy = RetryPolicy?.Clone(),
    };
}
