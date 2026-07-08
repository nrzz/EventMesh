namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Describes a publish/subscribe topic or exchange in broker topology.
/// </summary>
public sealed class TopicDefinition
{
    /// <summary>
    /// Gets or sets the topic or exchange name.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Gets or sets the topic type or exchange kind understood by the transport.
    /// </summary>
    public string? Type { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the topic is durable.
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether the topic is automatically deleted when unused.
    /// </summary>
    public bool AutoDelete { get; set; }

    /// <summary>
    /// Gets or sets the number of partitions for partitioned topics.
    /// </summary>
    public int? PartitionCount { get; set; }

    /// <summary>
    /// Gets or sets the replication factor for partitioned topics.
    /// </summary>
    public short? ReplicationFactor { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the topic enforces ordering.
    /// </summary>
    public bool Ordered { get; set; }

    /// <summary>
    /// Gets or sets transport-specific arguments for topic creation.
    /// </summary>
    public IDictionary<string, string> Arguments { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
