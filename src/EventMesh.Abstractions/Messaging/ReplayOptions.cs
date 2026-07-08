namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// Options that control replay of messages from a topic, stream, or archive.
/// </summary>
public sealed class ReplayOptions
{
    /// <summary>
    /// Gets or sets the source topic, stream, or archive from which messages are replayed.
    /// </summary>
    public string? Source { get; set; }

    /// <summary>
    /// Gets or sets the destination topic or queue to which replayed messages are published.
    /// When not set, messages are replayed to their original destination.
    /// </summary>
    public string? Destination { get; set; }

    /// <summary>
    /// Gets or sets the inclusive start time for replay.
    /// </summary>
    public DateTimeOffset? From { get; set; }

    /// <summary>
    /// Gets or sets the exclusive end time for replay.
    /// </summary>
    public DateTimeOffset? To { get; set; }

    /// <summary>
    /// Gets or sets the inclusive starting offset or sequence for replay.
    /// </summary>
    public long? FromOffset { get; set; }

    /// <summary>
    /// Gets or sets the exclusive ending offset or sequence for replay.
    /// </summary>
    public long? ToOffset { get; set; }

    /// <summary>
    /// Gets or sets a filter expression or message type name limiting replay scope.
    /// </summary>
    public string? Filter { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of messages to replay.
    /// </summary>
    public int? MaxMessages { get; set; }

    /// <summary>
    /// Gets or sets the batch size used when reading messages during replay.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Gets or sets a value indicating whether replayed messages preserve original envelope metadata.
    /// </summary>
    public bool PreserveMetadata { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether replay should run asynchronously in the background.
    /// </summary>
    public bool RunInBackground { get; set; }

    /// <summary>
    /// Gets or sets transport-specific or application headers applied to replayed messages.
    /// </summary>
    public IDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Creates a shallow copy of the current options instance.
    /// </summary>
    public ReplayOptions Clone() => new()
    {
        Source = Source,
        Destination = Destination,
        From = From,
        To = To,
        FromOffset = FromOffset,
        ToOffset = ToOffset,
        Filter = Filter,
        MaxMessages = MaxMessages,
        BatchSize = BatchSize,
        PreserveMetadata = PreserveMetadata,
        RunInBackground = RunInBackground,
        Headers = Headers is null ? null : new Dictionary<string, string>(Headers),
    };
}
