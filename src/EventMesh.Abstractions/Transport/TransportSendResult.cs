namespace EventMesh.Abstractions.Transport;

/// <summary>
/// The result of sending a message through a broker transport.
/// </summary>
public sealed class TransportSendResult
{
    /// <summary>
    /// Gets or sets a value indicating whether the send operation succeeded.
    /// </summary>
    public bool Succeeded { get; set; }

    /// <summary>
    /// Gets or sets the broker-assigned message identifier.
    /// </summary>
    public string? MessageId { get; set; }

    /// <summary>
    /// Gets or sets the sequence number or offset assigned by the broker, when available.
    /// </summary>
    public long? SequenceNumber { get; set; }

    /// <summary>
    /// Gets or sets the partition identifier, when available.
    /// </summary>
    public string? Partition { get; set; }

    /// <summary>
    /// Gets or sets transport-specific metadata returned by the broker.
    /// </summary>
    public IDictionary<string, string> Metadata { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets the error message when <see cref="Succeeded"/> is <see langword="false"/>.
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Creates a successful send result.
    /// </summary>
    public static TransportSendResult Success(string? messageId = null, long? sequenceNumber = null) => new()
    {
        Succeeded = true,
        MessageId = messageId,
        SequenceNumber = sequenceNumber,
    };

    /// <summary>
    /// Creates a failed send result.
    /// </summary>
    public static TransportSendResult Failure(string errorMessage) => new()
    {
        Succeeded = false,
        ErrorMessage = errorMessage,
    };
}
