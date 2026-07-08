namespace EventMesh.Transport.AzureServiceBus;

/// <summary>
/// Configuration options for the Azure Service Bus broker transport.
/// </summary>
public sealed class AzureServiceBusTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:AzureServiceBus";

    /// <summary>
    /// Gets or sets the Azure Service Bus connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the prefetch count used by receivers.
    /// </summary>
    public int PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Gets or sets the maximum number of concurrent message callbacks.
    /// </summary>
    public int MaxConcurrentCalls { get; set; } = 1;

    /// <summary>
    /// Gets or sets the maximum time to wait for a message during receive polling.
    /// </summary>
    public TimeSpan ReceiveWaitTime { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the delay between receive poll attempts when no message is available.
    /// </summary>
    public TimeSpan ReceivePollInterval { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Gets or sets the suffix appended to a queue name when no explicit dead-letter destination is configured.
    /// </summary>
    public string DefaultDeadLetterSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Gets or sets the default maximum delivery attempts before a message is dead-lettered.
    /// </summary>
    public int DefaultMaxDeliveryAttempts { get; set; } = 5;
}
