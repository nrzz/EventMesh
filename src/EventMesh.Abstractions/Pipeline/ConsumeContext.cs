using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Messaging;

namespace EventMesh.Abstractions.Pipeline;

/// <summary>
/// Context for a message being consumed through the filter pipeline.
/// </summary>
/// <typeparam name="T">The message payload type.</typeparam>
public sealed class ConsumeContext<T> : FilterContext where T : notnull
{
    /// <summary>
    /// Gets or sets the deserialized message payload.
    /// </summary>
    public required T Message { get; set; }

    /// <summary>
    /// Gets or sets the CloudEvents envelope associated with the message.
    /// </summary>
    public MessageEnvelope Envelope { get; set; } = null!;

    /// <summary>
    /// Gets or sets the subscription options for the consumer.
    /// </summary>
    public SubscribeOptions Options { get; set; } = new();

    /// <summary>
    /// Gets or sets the transport delivery tag used to acknowledge or reject the message.
    /// </summary>
    public string? DeliveryTag { get; set; }

    /// <summary>
    /// Gets or sets the number of times the message has been delivered.
    /// </summary>
    public int DeliveryCount { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the message has been acknowledged.
    /// </summary>
    public bool IsAcknowledged { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the message has been rejected.
    /// </summary>
    public bool IsRejected { get; set; }
}
