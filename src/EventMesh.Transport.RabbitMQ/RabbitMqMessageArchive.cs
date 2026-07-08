using System.Collections.Concurrent;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;

namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// Archives published messages to support emulated replay on RabbitMQ.
/// </summary>
internal sealed class RabbitMqMessageArchive
{
    private readonly ConcurrentDictionary<string, StoredTransportMessage> _messages = new(StringComparer.OrdinalIgnoreCase);
    private long _sequenceCounter;

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

    public Task<long> ReplayAsync(
        Func<TransportMessage, CancellationToken, Task<TransportSendResult>> publishAsync,
        ReplayOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(publishAsync);
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();

        return ReplayCoreAsync(publishAsync, options, cancellationToken);
    }

    private async Task<long> ReplayCoreAsync(
        Func<TransportMessage, CancellationToken, Task<TransportSendResult>> publishAsync,
        ReplayOptions options,
        CancellationToken cancellationToken)
    {
        var candidates = Query(options).ToList();
        var replayed = 0L;

        foreach (var stored in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var destination = options.Destination ?? stored.Destination;
            var replayMessage = CloneMessage(stored.Message, Guid.NewGuid().ToString("N"));
            replayMessage.Destination = destination;
            replayMessage.Headers["x-eventmesh-replay"] = "true";
            replayMessage.Headers["x-eventmesh-original-message-id"] = stored.MessageId;

            if (options.Headers is not null)
            {
                foreach (var header in options.Headers)
                {
                    replayMessage.Headers[header.Key] = header.Value;
                }
            }

            var result = await publishAsync(replayMessage, cancellationToken);
            if (!result.Succeeded)
            {
                throw new InvalidOperationException(
                    $"Replay publish failed for message '{stored.MessageId}': {result.ErrorMessage}");
            }

            replayed++;
        }

        return replayed;
    }

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

internal sealed class StoredTransportMessage
{
    public required string MessageId { get; init; }

    public long SequenceNumber { get; init; }

    public required string Destination { get; init; }

    public DateTimeOffset EnqueuedAt { get; init; }

    public required TransportMessage Message { get; init; }
}
