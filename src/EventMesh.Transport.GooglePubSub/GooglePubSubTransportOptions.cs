using Google.Api.Gax;

namespace EventMesh.Transport.GooglePubSub;

/// <summary>
/// Configuration options for the Google Pub/Sub broker transport.
/// </summary>
public sealed class GooglePubSubTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:GooglePubSub";

    /// <summary>
    /// Gets or sets the Google Cloud project identifier.
    /// </summary>
    public string ProjectId { get; set; } = "eventmesh";

    /// <summary>
    /// Gets or sets how the transport detects and connects to the Pub/Sub emulator.
    /// </summary>
    public EmulatorDetection EmulatorDetection { get; set; } = EmulatorDetection.EmulatorOrProduction;

    /// <summary>
    /// Gets or sets the interval used while polling for messages.
    /// </summary>
    public TimeSpan ReceivePollInterval { get; set; } = TimeSpan.FromMilliseconds(100);

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
    /// Gets or sets the acknowledgement deadline in seconds for pull subscriptions.
    /// </summary>
    public int AckDeadlineSeconds { get; set; } = 60;

    /// <summary>
    /// Gets or sets a value indicating whether message ordering keys are enabled.
    /// </summary>
    public bool EnableMessageOrdering { get; set; } = true;

    /// <summary>
    /// Gets a value indicating whether the Pub/Sub emulator is currently configured.
    /// </summary>
    public bool IsEmulatorDetected =>
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST"));
}
