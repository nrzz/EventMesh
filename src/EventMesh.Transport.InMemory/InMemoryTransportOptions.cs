namespace EventMesh.Transport.InMemory;

/// <summary>
/// Configuration options for the in-memory broker transport.
/// </summary>
public sealed class InMemoryTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:InMemory";

    /// <summary>
    /// Gets or sets the polling interval used while waiting for messages during receive operations.
    /// </summary>
    public TimeSpan ReceivePollInterval { get; set; } = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Gets or sets the interval used to promote delayed messages to their destination queues.
    /// </summary>
    public TimeSpan DelayCheckInterval { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Gets or sets the maximum supported message priority level.
    /// </summary>
    public int MaxPriority { get; set; } = 10;

    /// <summary>
    /// Gets or sets the suffix appended to a queue name when no explicit dead-letter destination is configured.
    /// </summary>
    public string DefaultDeadLetterSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Gets or sets the default maximum delivery attempts before a message is dead-lettered.
    /// </summary>
    public int DefaultMaxDeliveryAttempts { get; set; } = 5;
}
