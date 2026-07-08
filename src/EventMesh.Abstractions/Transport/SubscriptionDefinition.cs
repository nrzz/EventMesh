namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Describes a subscription binding between a topic and a queue or consumer.
/// </summary>
public sealed class SubscriptionDefinition
{
    /// <summary>
    /// Gets or sets the subscription name.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Gets or sets the source topic or exchange name.
    /// </summary>
    public required string Topic { get; set; }

    /// <summary>
    /// Gets or sets the destination queue or consumer group.
    /// </summary>
    public string? Destination { get; set; }

    /// <summary>
    /// Gets or sets the routing key or filter expression.
    /// </summary>
    public string? Filter { get; set; }

    /// <summary>
    /// Gets or sets the consumer group name for load-balanced subscriptions.
    /// </summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the subscription is durable.
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Gets or sets transport-specific arguments for subscription creation.
    /// </summary>
    public IDictionary<string, string> Arguments { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
