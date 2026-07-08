namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Describes the messaging topology to provision on a broker transport.
/// </summary>
public sealed class TopologyDefinition
{
    /// <summary>
    /// Gets or sets the topology name used for identification and logging.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Gets or sets the queues to create.
    /// </summary>
    public IList<QueueDefinition> Queues { get; set; } = new List<QueueDefinition>();

    /// <summary>
    /// Gets or sets the topics or exchanges to create.
    /// </summary>
    public IList<TopicDefinition> Topics { get; set; } = new List<TopicDefinition>();

    /// <summary>
    /// Gets or sets the subscriptions or bindings to create.
    /// </summary>
    public IList<SubscriptionDefinition> Subscriptions { get; set; } = new List<SubscriptionDefinition>();

    /// <summary>
    /// Gets or sets a value indicating whether missing entities should be deleted before creation.
    /// </summary>
    public bool ReplaceExisting { get; set; }

    /// <summary>
    /// Gets or sets transport-specific arguments applied to the topology operation.
    /// </summary>
    public IDictionary<string, string> Arguments { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
