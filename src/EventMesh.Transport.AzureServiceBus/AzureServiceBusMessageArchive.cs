using System.Collections.Concurrent;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;

namespace EventMesh.Transport.AzureServiceBus;

/// <summary>
/// Stores sent messages so they can be replayed through Azure Service Bus.
/// </summary>
internal sealed class AzureServiceBusMessageArchive
{
    private readonly ConcurrentDictionary<string, ConcurrentQueue<TransportMessage>> _messages = new(StringComparer.OrdinalIgnoreCase);

    public void Record(TransportMessage message, string destination)
    {
        var archived = CloneMessage(message);
        archived.Destination = destination;
        archived.Headers["x-eventmesh-replay"] = "true";

        var queue = _messages.GetOrAdd(destination, static _ => new ConcurrentQueue<TransportMessage>());
        queue.Enqueue(archived);
    }

    public async Task<long> ReplayAsync(
        Func<TransportMessage, CancellationToken, Task<TransportSendResult>> publish,
        ReplayOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(publish);
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.Source))
        {
            return 0;
        }

        if (!_messages.TryGetValue(options.Source, out var archivedMessages))
        {
            return 0;
        }

        var replayed = 0L;
        foreach (var message in archivedMessages.ToArray())
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (options.From is not null && message.EnqueuedAt is not null && message.EnqueuedAt < options.From)
            {
                continue;
            }

            if (options.To is not null && message.EnqueuedAt is not null && message.EnqueuedAt > options.To)
            {
                continue;
            }

            var replayMessage = CloneMessage(message);
            replayMessage.MessageId = Guid.NewGuid().ToString("N");
            replayMessage.Headers["x-eventmesh-replay"] = "true";

            var result = await publish(replayMessage, cancellationToken);
            if (!result.Succeeded)
            {
                throw new InvalidOperationException(
                    $"Failed to replay message to '{replayMessage.Destination}': {result.ErrorMessage}");
            }

            replayed++;
        }

        return replayed;
    }

    private static TransportMessage CloneMessage(TransportMessage source) => new()
    {
        MessageId = source.MessageId,
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
        EnqueuedAt = source.EnqueuedAt ?? DateTimeOffset.UtcNow,
    };
}
