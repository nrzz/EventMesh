using System.Collections.Concurrent;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;

namespace EventMesh.Transport.InMemory;

/// <summary>
/// Stores all messages published through the in-memory transport for replay.
/// </summary>
public sealed class InMemoryMessageStore
{
    private readonly ConcurrentDictionary<string, StoredTransportMessage> _messages = new(StringComparer.OrdinalIgnoreCase);
    private long _sequenceCounter;

    /// <summary>
    /// Records a message that was published to the broker.
    /// </summary>
    public StoredTransportMessage Record(TransportMessage message, string destination)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);

        var messageId = message.MessageId ?? Guid.NewGuid().ToString("N");
        var sequenceNumber = Interlocked.Increment(ref _sequenceCounter);
        var stored = new StoredTransportMessage
        {
            MessageId = messageId,
            SequenceNumber = sequenceNumber,
            Destination = destination,
            EnqueuedAt = DateTimeOffset.UtcNow,
            Message = CloneMessage(message, messageId),
        };

        _messages[messageId] = stored;
        return stored;
    }

    /// <summary>
    /// Gets all stored messages in sequence order.
    /// </summary>
    public IReadOnlyList<StoredTransportMessage> GetAll() =>
        _messages.Values.OrderBy(message => message.SequenceNumber).ToList();

    /// <summary>
    /// Replays stored messages matching the provided options by re-enqueueing them on the broker.
    /// </summary>
    public Task<long> ReplayAsync(
        InMemoryBrokerState brokerState,
        ReplayOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(brokerState);
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();

        var candidates = Query(options).ToList();
        var replayed = 0L;

        foreach (var stored in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var destination = options.Destination ?? stored.Destination;
            var replayMessage = CloneMessage(stored.Message, Guid.NewGuid().ToString("N"));
            replayMessage.Headers["x-eventmesh-replay"] = "true";
            replayMessage.Headers["x-eventmesh-original-message-id"] = stored.MessageId;

            if (options.Headers is not null)
            {
                foreach (var header in options.Headers)
                {
                    replayMessage.Headers[header.Key] = header.Value;
                }
            }

            brokerState.Enqueue(destination, replayMessage, recordInStore: false);
            replayed++;
        }

        return Task.FromResult(replayed);
    }

    /// <summary>
    /// Clears all stored messages.
    /// </summary>
    public void Clear() => _messages.Clear();

    private IEnumerable<StoredTransportMessage> Query(ReplayOptions options)
    {
        IEnumerable<StoredTransportMessage> query = _messages.Values;

        if (!string.IsNullOrWhiteSpace(options.Source))
        {
            query = query.Where(message =>
                string.Equals(message.Destination, options.Source, StringComparison.OrdinalIgnoreCase));
        }

        if (options.From is not null)
        {
            query = query.Where(message => message.EnqueuedAt >= options.From.Value);
        }

        if (options.To is not null)
        {
            query = query.Where(message => message.EnqueuedAt < options.To.Value);
        }

        if (options.FromOffset is not null)
        {
            query = query.Where(message => message.SequenceNumber >= options.FromOffset.Value);
        }

        if (options.ToOffset is not null)
        {
            query = query.Where(message => message.SequenceNumber < options.ToOffset.Value);
        }

        query = query.OrderBy(message => message.SequenceNumber);

        if (options.MaxMessages is not null)
        {
            query = query.Take(options.MaxMessages.Value);
        }

        return query;
    }

    private static TransportMessage CloneMessage(TransportMessage source, string messageId) => new()
    {
        MessageId = messageId,
        Destination = source.Destination,
        RoutingKey = source.RoutingKey,
        Body = source.Body.ToArray(),
        ContentType = source.ContentType,
        Headers = new Dictionary<string, string>(source.Headers, StringComparer.OrdinalIgnoreCase),
        CorrelationId = source.CorrelationId,
        ReplyTo = source.ReplyTo,
        Priority = source.Priority,
        TimeToLive = source.TimeToLive,
        ScheduledAt = source.ScheduledAt,
        SessionId = source.SessionId,
        PartitionKey = source.PartitionKey,
        DeliveryCount = source.DeliveryCount,
        EnqueuedAt = source.EnqueuedAt,
    };
}

/// <summary>
/// A message persisted by <see cref="InMemoryMessageStore"/>.
/// </summary>
public sealed class StoredTransportMessage
{
    /// <summary>
    /// Gets or sets the message identifier.
    /// </summary>
    public required string MessageId { get; init; }

    /// <summary>
    /// Gets or sets the monotonically increasing sequence number.
    /// </summary>
    public long SequenceNumber { get; init; }

    /// <summary>
    /// Gets or sets the destination where the message was originally published.
    /// </summary>
    public required string Destination { get; init; }

    /// <summary>
    /// Gets or sets the time when the message was stored.
    /// </summary>
    public DateTimeOffset EnqueuedAt { get; init; }

    /// <summary>
    /// Gets or sets the stored transport message.
    /// </summary>
    public required TransportMessage Message { get; init; }
}
