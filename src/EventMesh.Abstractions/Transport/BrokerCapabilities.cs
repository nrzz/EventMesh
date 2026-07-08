namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Describes optional capabilities supported by a broker transport implementation.
/// </summary>
[Flags]
public enum BrokerCapabilities : ulong
{
    /// <summary>
    /// No capabilities are supported.
    /// </summary>
    None = 0,

    /// <summary>
    /// The broker can delay message delivery until a specified time.
    /// </summary>
    DelayedDelivery = 1UL << 0,

    /// <summary>
    /// The broker supports message priority levels.
    /// </summary>
    Priority = 1UL << 1,

    /// <summary>
    /// The broker supports replaying messages from a historical position.
    /// </summary>
    Replay = 1UL << 2,

    /// <summary>
    /// The broker supports session-based message grouping.
    /// </summary>
    Sessions = 1UL << 3,

    /// <summary>
    /// The broker supports transactional send and receive operations.
    /// </summary>
    Transactions = 1UL << 4,

    /// <summary>
    /// The broker supports dead-letter queues or topics.
    /// </summary>
    DeadLettering = 1UL << 5,

    /// <summary>
    /// The broker preserves message ordering within a scope.
    /// </summary>
    Ordering = 1UL << 6,

    /// <summary>
    /// The broker supports consumer groups for load-balanced consumption.
    /// </summary>
    ConsumerGroups = 1UL << 7,

    /// <summary>
    /// The broker supports routing keys or binding patterns.
    /// </summary>
    RoutingKeys = 1UL << 8,

    /// <summary>
    /// The broker supports partitioned topics or streams.
    /// </summary>
    Partitions = 1UL << 9,

    /// <summary>
    /// The broker supports first-in-first-out queues or topics.
    /// </summary>
    Fifo = 1UL << 10,

    /// <summary>
    /// The broker supports visibility timeouts for in-flight messages.
    /// </summary>
    VisibilityTimeout = 1UL << 11,

    /// <summary>
    /// The broker supports native message scheduling without emulation.
    /// </summary>
    NativeScheduling = 1UL << 12,

    /// <summary>
    /// The broker supports per-message time-to-live.
    /// </summary>
    Ttl = 1UL << 13,

    /// <summary>
    /// The broker supports publisher confirms or equivalent delivery acknowledgements.
    /// </summary>
    PublisherConfirms = 1UL << 14,

    /// <summary>
    /// The broker supports request/response messaging patterns.
    /// </summary>
    RequestResponse = 1UL << 15,

    /// <summary>
    /// The broker supports message deduplication.
    /// </summary>
    Deduplication = 1UL << 16,

    /// <summary>
    /// The broker persists messages to durable storage.
    /// </summary>
    MessagePersistence = 1UL << 17,

    /// <summary>
    /// The broker supports streaming consumption models.
    /// </summary>
    Streaming = 1UL << 18,

    /// <summary>
    /// The broker supports receiving messages in batches.
    /// </summary>
    BatchReceive = 1UL << 19,

    /// <summary>
    /// The broker supports sending messages in batches.
    /// </summary>
    BatchSend = 1UL << 20,

    /// <summary>
    /// The broker supports exponential backoff for retries.
    /// </summary>
    ExponentialBackoff = 1UL << 21,

    /// <summary>
    /// The broker exposes telemetry or tracing hooks.
    /// </summary>
    Telemetry = 1UL << 22,

    /// <summary>
    /// The broker supports topic-based publish/subscribe.
    /// </summary>
    PubSub = 1UL << 23,

    /// <summary>
    /// The broker supports point-to-point queue semantics.
    /// </summary>
    Queues = 1UL << 24,

    /// <summary>
    /// The broker supports message headers or user properties.
    /// </summary>
    MessageHeaders = 1UL << 25,

    /// <summary>
    /// The broker supports automatic topology provisioning.
    /// </summary>
    TopologyProvisioning = 1UL << 26,

    /// <summary>
    /// The broker supports subscription filters.
    /// </summary>
    SubscriptionFilters = 1UL << 27,

    /// <summary>
    /// The broker supports compression of message payloads.
    /// </summary>
    Compression = 1UL << 28,

    /// <summary>
    /// The broker supports large message payloads via chunking or blob storage.
    /// </summary>
    LargePayloads = 1UL << 29,

    /// <summary>
    /// The broker exposes pending message lists and claim-based recovery.
    /// </summary>
    PendingMessages = 1UL << 30,
}
