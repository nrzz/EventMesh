namespace EventMesh.Abstractions.Transport;

/// <summary>
/// The result of receiving a message from a broker transport.
/// </summary>
public sealed class TransportReceiveResult
{
    /// <summary>
    /// Gets or sets a value indicating whether a message was received.
    /// </summary>
    public bool HasMessage { get; set; }

    /// <summary>
    /// Gets or sets the received message when <see cref="HasMessage"/> is <see langword="true"/>.
    /// </summary>
    public TransportMessage? Message { get; set; }

    /// <summary>
    /// Gets or sets the delivery tag used to acknowledge or reject the message.
    /// </summary>
    public string? DeliveryTag { get; set; }

    /// <summary>
    /// Gets or sets transport-specific metadata associated with the receive operation.
    /// </summary>
    public IDictionary<string, string> Metadata { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Creates a result indicating that no message was available.
    /// </summary>
    public static TransportReceiveResult Empty() => new() { HasMessage = false };

    /// <summary>
    /// Creates a result containing a received message.
    /// </summary>
    public static TransportReceiveResult Received(TransportMessage message, string? deliveryTag = null) => new()
    {
        HasMessage = true,
        Message = message,
        DeliveryTag = deliveryTag ?? message.DeliveryTag,
    };
}
