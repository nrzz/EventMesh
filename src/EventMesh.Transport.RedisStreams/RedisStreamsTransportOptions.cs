namespace EventMesh.Transport.RedisStreams;

/// <summary>
/// Configuration options for the Redis Streams broker transport.
/// </summary>
public sealed class RedisStreamsTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:RedisStreams";

    /// <summary>
    /// Gets or sets the Redis connection string.
    /// </summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Gets or sets the prefix applied to all stream keys.
    /// </summary>
    public string StreamPrefix { get; set; } = "eventmesh:";

    /// <summary>
    /// Gets or sets the consumer name used for stream read group operations.
    /// </summary>
    public string? ConsumerName { get; set; }

    /// <summary>
    /// Gets or sets the polling interval used while waiting for messages during receive operations.
    /// </summary>
    public TimeSpan ReceivePollInterval { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Gets or sets the block duration passed to stream read group operations.
    /// </summary>
    public TimeSpan ReceiveBlockDuration { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Gets or sets the minimum idle time before pending messages are auto-claimed.
    /// </summary>
    public TimeSpan PendingClaimMinIdle { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the interval used to promote delayed messages to their destination streams.
    /// </summary>
    public TimeSpan DelayCheckInterval { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Gets or sets the suffix appended to a queue name when no explicit dead-letter destination is configured.
    /// </summary>
    public string DefaultDeadLetterSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Gets or sets the default maximum delivery attempts before a message is dead-lettered.
    /// </summary>
    public int DefaultMaxDeliveryAttempts { get; set; } = 5;

    /// <summary>
    /// Gets or sets the maximum supported message priority level.
    /// </summary>
    public int MaxPriority { get; set; } = 10;

    /// <summary>
    /// Gets or sets the Redis key used for delayed message scheduling.
    /// </summary>
    public string DelayedMessagesKey { get; set; } = "eventmesh:delayed";
}
