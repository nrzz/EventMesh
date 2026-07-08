namespace EventMesh.Abstractions.Envelope;

/// <summary>
/// CloudEvents 1.0 compliant message envelope used as the canonical wire format for EventMesh.
/// </summary>
public sealed class MessageEnvelope
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the CloudEvents specification version.
    /// </summary>
    public string SpecVersion { get; init; } = "1.0";

    /// <summary>
    /// Gets the unique identifier for the event.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the event source URI-reference.
    /// </summary>
    public required string Source { get; init; }

    /// <summary>
    /// Gets the event type identifier.
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Gets the content type of the <see cref="Data"/> attribute.
    /// </summary>
    public string? DataContentType { get; init; }

    /// <summary>
    /// Gets the serialized event payload.
    /// </summary>
    public ReadOnlyMemory<byte>? Data { get; init; }

    /// <summary>
    /// Gets the timestamp when the event occurred.
    /// </summary>
    public DateTimeOffset? Time { get; init; }

    /// <summary>
    /// Gets the subject of the event in the context of the event producer.
    /// </summary>
    public string? Subject { get; init; }

    /// <summary>
    /// Gets the correlation identifier for distributed tracing and request/response flows.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the causation identifier linking this event to the event that caused it.
    /// </summary>
    public string? CausationId { get; init; }

    /// <summary>
    /// Gets extension attributes and transport headers associated with the envelope.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = EmptyHeaders;

    /// <summary>
    /// Creates a new envelope builder pre-populated with a generated identifier and current UTC time.
    /// </summary>
    public static MessageEnvelopeBuilder Create() => new();

    /// <summary>
    /// Creates a new envelope builder seeded from an existing envelope.
    /// </summary>
    public static MessageEnvelopeBuilder From(MessageEnvelope envelope) => new(envelope);

    /// <summary>
    /// Returns a copy of the envelope with updated headers.
    /// </summary>
    public MessageEnvelope WithHeaders(IReadOnlyDictionary<string, string> headers) => new()
    {
        SpecVersion = SpecVersion,
        Id = Id,
        Source = Source,
        Type = Type,
        DataContentType = DataContentType,
        Data = Data,
        Time = Time,
        Subject = Subject,
        CorrelationId = CorrelationId,
        CausationId = CausationId,
        Headers = headers,
    };
}
