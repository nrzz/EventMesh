namespace EventMesh.Transport.Nats;

/// <summary>
/// Configuration options for the NATS JetStream broker transport.
/// </summary>
public sealed class NatsTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:Nats";

    /// <summary>
    /// Gets or sets the NATS server URL.
    /// </summary>
    public string Url { get; set; } = "nats://localhost:4222";

    /// <summary>
    /// Gets or sets the authentication token.
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// Gets or sets the authentication username.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Gets or sets the authentication password.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Gets or sets the credentials file path.
    /// </summary>
    public string? CredentialsFile { get; set; }

    /// <summary>
    /// Gets or sets the default durable consumer prefix.
    /// </summary>
    public string ConsumerPrefix { get; set; } = "eventmesh";

    /// <summary>
    /// Gets or sets the prefix applied to JetStream stream names.
    /// </summary>
    public string StreamPrefix { get; set; } = "EM_";

    /// <summary>
    /// Gets or sets the timeout used while fetching messages from consumers.
    /// </summary>
    public TimeSpan FetchTimeout { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Gets or sets the interval used to promote delayed messages.
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

    /// <summary>
    /// Gets or sets the maximum number of messages requested per fetch operation.
    /// </summary>
    public int FetchBatchSize { get; set; } = 1;
}
