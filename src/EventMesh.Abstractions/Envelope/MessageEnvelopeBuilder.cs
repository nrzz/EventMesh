namespace EventMesh.Abstractions.Envelope;

/// <summary>
/// Fluent builder for constructing <see cref="MessageEnvelope"/> instances.
/// </summary>
public sealed class MessageEnvelopeBuilder
{
    private string _specVersion = "1.0";
    private string? _id;
    private string? _source;
    private string? _type;
    private string? _dataContentType;
    private ReadOnlyMemory<byte>? _data;
    private DateTimeOffset? _time;
    private string? _subject;
    private string? _correlationId;
    private string? _causationId;
    private readonly Dictionary<string, string> _headers = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageEnvelopeBuilder"/> class.
    /// </summary>
    public MessageEnvelopeBuilder()
    {
        _id = Guid.NewGuid().ToString("N");
        _time = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Initializes a new builder seeded from an existing envelope.
    /// </summary>
    public MessageEnvelopeBuilder(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        _specVersion = envelope.SpecVersion;
        _id = envelope.Id;
        _source = envelope.Source;
        _type = envelope.Type;
        _dataContentType = envelope.DataContentType;
        _data = envelope.Data;
        _time = envelope.Time;
        _subject = envelope.Subject;
        _correlationId = envelope.CorrelationId;
        _causationId = envelope.CausationId;

        foreach (var header in envelope.Headers)
        {
            _headers[header.Key] = header.Value;
        }
    }

    /// <summary>
    /// Sets the CloudEvents specification version.
    /// </summary>
    public MessageEnvelopeBuilder WithSpecVersion(string specVersion)
    {
        _specVersion = specVersion;
        return this;
    }

    /// <summary>
    /// Sets the envelope identifier.
    /// </summary>
    public MessageEnvelopeBuilder WithId(string id)
    {
        _id = id;
        return this;
    }

    /// <summary>
    /// Sets the event source URI-reference.
    /// </summary>
    public MessageEnvelopeBuilder WithSource(string source)
    {
        _source = source;
        return this;
    }

    /// <summary>
    /// Sets the event type.
    /// </summary>
    public MessageEnvelopeBuilder WithType(string type)
    {
        _type = type;
        return this;
    }

    /// <summary>
    /// Sets the payload content type.
    /// </summary>
    public MessageEnvelopeBuilder WithDataContentType(string? dataContentType)
    {
        _dataContentType = dataContentType;
        return this;
    }

    /// <summary>
    /// Sets the serialized payload.
    /// </summary>
    public MessageEnvelopeBuilder WithData(ReadOnlyMemory<byte>? data)
    {
        _data = data;
        return this;
    }

    /// <summary>
    /// Sets the serialized payload from a byte array.
    /// </summary>
    public MessageEnvelopeBuilder WithData(byte[]? data)
    {
        _data = data;
        return this;
    }

    /// <summary>
    /// Sets the event timestamp.
    /// </summary>
    public MessageEnvelopeBuilder WithTime(DateTimeOffset? time)
    {
        _time = time;
        return this;
    }

    /// <summary>
    /// Sets the event subject.
    /// </summary>
    public MessageEnvelopeBuilder WithSubject(string? subject)
    {
        _subject = subject;
        return this;
    }

    /// <summary>
    /// Sets the correlation identifier.
    /// </summary>
    public MessageEnvelopeBuilder WithCorrelationId(string? correlationId)
    {
        _correlationId = correlationId;
        return this;
    }

    /// <summary>
    /// Sets the causation identifier.
    /// </summary>
    public MessageEnvelopeBuilder WithCausationId(string? causationId)
    {
        _causationId = causationId;
        return this;
    }

    /// <summary>
    /// Adds or replaces a header value.
    /// </summary>
    public MessageEnvelopeBuilder WithHeader(string name, string value)
    {
        _headers[name] = value;
        return this;
    }

    /// <summary>
    /// Adds or replaces multiple header values.
    /// </summary>
    public MessageEnvelopeBuilder WithHeaders(IEnumerable<KeyValuePair<string, string>> headers)
    {
        foreach (var header in headers)
        {
            _headers[header.Key] = header.Value;
        }

        return this;
    }

    /// <summary>
    /// Builds the envelope, validating required CloudEvents attributes.
    /// </summary>
    public MessageEnvelope Build()
    {
        if (string.IsNullOrWhiteSpace(_id))
        {
            throw new InvalidOperationException("Envelope Id is required.");
        }

        if (string.IsNullOrWhiteSpace(_source))
        {
            throw new InvalidOperationException("Envelope Source is required.");
        }

        if (string.IsNullOrWhiteSpace(_type))
        {
            throw new InvalidOperationException("Envelope Type is required.");
        }

        if (string.IsNullOrWhiteSpace(_specVersion))
        {
            throw new InvalidOperationException("Envelope SpecVersion is required.");
        }

        return new MessageEnvelope
        {
            SpecVersion = _specVersion,
            Id = _id,
            Source = _source,
            Type = _type,
            DataContentType = _dataContentType,
            Data = _data,
            Time = _time,
            Subject = _subject,
            CorrelationId = _correlationId,
            CausationId = _causationId,
            Headers = new Dictionary<string, string>(_headers, StringComparer.OrdinalIgnoreCase),
        };
    }
}
