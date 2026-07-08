using Confluent.Kafka;

namespace EventMesh.Transport.Kafka;

/// <summary>
/// Configuration options for the Kafka broker transport.
/// </summary>
public sealed class KafkaTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:Kafka";

    /// <summary>
    /// Gets or sets the Kafka bootstrap servers.
    /// </summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Gets or sets the default consumer group identifier.
    /// </summary>
    public string GroupId { get; set; } = "eventmesh";

    /// <summary>
    /// Gets or sets the producer acknowledgement mode.
    /// </summary>
    public Acks Acks { get; set; } = Acks.All;

    /// <summary>
    /// Gets or sets the producer compression type.
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary>
    /// Gets or sets the default number of partitions created for topics.
    /// </summary>
    public int DefaultPartitionCount { get; set; } = 3;

    /// <summary>
    /// Gets or sets the replication factor used when creating topics.
    /// </summary>
    public short ReplicationFactor { get; set; } = 1;

    /// <summary>
    /// Gets or sets the consumer poll interval used while waiting for messages.
    /// </summary>
    public TimeSpan ConsumerPollInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the interval used to promote delayed messages to Kafka.
    /// </summary>
    public TimeSpan DelayCheckInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Gets or sets the suffix appended to a queue name when no explicit dead-letter destination is configured.
    /// </summary>
    public string DefaultDeadLetterSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Gets or sets the default maximum delivery attempts before a message is dead-lettered.
    /// </summary>
    public int DefaultMaxDeliveryAttempts { get; set; } = 5;
}
