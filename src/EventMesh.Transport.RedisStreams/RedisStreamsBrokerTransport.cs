using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventMesh.Transport.RedisStreams;

/// <summary>
/// Redis Streams implementation of <see cref="IBrokerTransport"/>.
/// </summary>
public sealed class RedisStreamsBrokerTransport : IBrokerTransport
{
    private readonly IConnectionMultiplexer _connection;
    private readonly RedisStreamsTopologyManager _topologyManager;
    private readonly RedisStreamsTransportOptions _options;
    private readonly ILogger<RedisStreamsBrokerTransport> _logger;
    private readonly string _consumerName;
    private readonly Timer _delayTimer;
    private readonly object _delayLock = new();
    private int _disposed;

    public RedisStreamsBrokerTransport(
        IConnectionMultiplexer connection,
        RedisStreamsTopologyManager topologyManager,
        IOptions<RedisStreamsTransportOptions> options,
        ILogger<RedisStreamsBrokerTransport> logger)
    {
        _connection = connection;
        _topologyManager = topologyManager;
        _options = options.Value;
        _logger = logger;
        _consumerName = string.IsNullOrWhiteSpace(_options.ConsumerName)
            ? $"{Environment.MachineName}-{Guid.NewGuid():N}"
            : _options.ConsumerName;
        _delayTimer = new Timer(PromoteDelayedMessages, null, _options.DelayCheckInterval, _options.DelayCheckInterval);
    }

    /// <inheritdoc />
    public string Name => "redisstreams";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities() =>
        BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.PendingMessages
        | BrokerCapabilities.DelayedDelivery
        | BrokerCapabilities.Priority
        | BrokerCapabilities.RequestResponse
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.Queues
        | BrokerCapabilities.MessageHeaders
        | BrokerCapabilities.TopologyProvisioning
        | BrokerCapabilities.Ordering
        | BrokerCapabilities.Fifo
        | BrokerCapabilities.MessagePersistence
        | BrokerCapabilities.PublisherConfirms
        | BrokerCapabilities.Streaming
        | BrokerCapabilities.VisibilityTimeout;

    /// <inheritdoc />
    public async Task<TransportSendResult> SendAsync(
        TransportMessage message,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(message);

        if (string.IsNullOrWhiteSpace(message.Destination))
        {
            return TransportSendResult.Failure("Destination is required.");
        }

        if (_topologyManager.IsTopic(message.Destination))
        {
            return await FanOutToTopicAsync(message, cancellationToken);
        }

        return await EnqueueAsync(message.Destination, message, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<TransportReceiveResult> ReceiveAsync(
        string queueOrSubscription,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(queueOrSubscription);

        var receiveTarget = _topologyManager.ResolveReceiveTarget(queueOrSubscription);

        while (!cancellationToken.IsCancellationRequested)
        {
            var claimed = await TryAutoClaimPendingAsync(
                receiveTarget.StreamKey,
                receiveTarget.ConsumerGroup,
                cancellationToken);

            if (claimed is not null)
            {
                return claimed;
            }

            claimed = await TryClaimPendingAsync(
                receiveTarget.StreamKey,
                receiveTarget.ConsumerGroup,
                cancellationToken);

            if (claimed is not null)
            {
                return claimed;
            }

            for (var priority = _options.MaxPriority; priority >= 0; priority--)
            {
                var priorityStream = _topologyManager.ToPriorityStreamKey(receiveTarget.QueueName, priority);
                var priorityResult = await TryReadGroupAsync(
                    priorityStream,
                    receiveTarget.ConsumerGroup,
                    receiveTarget.QueueName,
                    cancellationToken);

                if (priorityResult is not null)
                {
                    return priorityResult;
                }
            }

            var result = await TryReadGroupAsync(
                receiveTarget.StreamKey,
                receiveTarget.ConsumerGroup,
                receiveTarget.QueueName,
                cancellationToken);

            if (result is not null)
            {
                return result;
            }

            await Task.Delay(_options.ReceivePollInterval, cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return TransportReceiveResult.Empty();
    }

    /// <inheritdoc />
    public async Task AcknowledgeAsync(string deliveryTag, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(deliveryTag);

        if (!RedisStreamsMessageCodec.TryDecodeDeliveryTag(
                deliveryTag,
                out var streamKey,
                out var consumerGroup,
                out var messageId,
                out _))
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is invalid.");
        }

        var database = _connection.GetDatabase();
        var acknowledged = await database.StreamAcknowledgeAsync(streamKey, consumerGroup, messageId)
            .WaitAsync(cancellationToken);

        if (acknowledged == 0)
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is not in flight.");
        }
    }

    /// <inheritdoc />
    public async Task RejectAsync(
        string deliveryTag,
        bool requeue = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(deliveryTag);

        if (!RedisStreamsMessageCodec.TryDecodeDeliveryTag(
                deliveryTag,
                out var streamKey,
                out var consumerGroup,
                out var messageId,
                out _))
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is invalid.");
        }

        var database = _connection.GetDatabase();
        var entries = await database.StreamRangeAsync(streamKey, messageId, messageId, count: 1)
            .WaitAsync(cancellationToken);

        if (entries.Length == 0)
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is not in flight.");
        }

        var message = RedisStreamsMessageCodec.FromStreamEntry(entries[0]);
        var queueName = ResolveQueueNameFromStream(streamKey);
        var queueState = _topologyManager.GetQueueState(queueName);
        var nextDeliveryCount = message.DeliveryCount + 1;
        message.DeliveryCount = nextDeliveryCount;

        await database.StreamAcknowledgeAsync(streamKey, consumerGroup, messageId).WaitAsync(cancellationToken);

        if (requeue && nextDeliveryCount < queueState.MaxDeliveryAttempts)
        {
            message.MessageId = Guid.NewGuid().ToString("N");
            await PublishToStreamAsync(streamKey, message, cancellationToken);
            return;
        }

        message.Headers["x-eventmesh-dead-letter-reason"] = requeue
            ? "max-delivery-attempts-exceeded"
            : "rejected-without-requeue";

        var deadLetterStream = _topologyManager.ToStreamKey(queueState.DeadLetterDestination);
        await PublishToStreamAsync(deadLetterStream, message, cancellationToken);
    }

    /// <inheritdoc />
    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return _topologyManager.CreateTopologyAsync(topology, cancellationToken);
    }

    /// <summary>
    /// Replays messages from a Redis stream using XRANGE and re-publishes them to the destination stream.
    /// </summary>
    public async Task<long> ReplayAsync(ReplayOptions options, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(options.Source))
        {
            throw new ArgumentException("Replay source is required.", nameof(options));
        }

        var sourceStream = _topologyManager.ToStreamKey(options.Source);
        var destinationName = options.Destination ?? options.Source;
        var destinationStream = _topologyManager.ToStreamKey(destinationName);
        var database = _connection.GetDatabase();

        var startId = options.FromOffset.HasValue
            ? $"{options.FromOffset.Value}-0"
            : "-";

        var endId = options.ToOffset.HasValue
            ? $"{options.ToOffset.Value}-0"
            : "+";

        var entries = await database.StreamRangeAsync(
                sourceStream,
                minId: startId,
                maxId: endId,
                count: options.MaxMessages ?? int.MaxValue)
            .WaitAsync(cancellationToken);

        var replayed = 0L;
        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (entry.Values.Length == 1
                && entry.Values[0].Name == "__bootstrap")
            {
                continue;
            }

            if (options.From is not null || options.To is not null)
            {
                var message = RedisStreamsMessageCodec.FromStreamEntry(entry);
                if (options.From is not null && message.EnqueuedAt < options.From)
                {
                    continue;
                }

                if (options.To is not null && message.EnqueuedAt >= options.To)
                {
                    continue;
                }
            }

            var replayMessage = RedisStreamsMessageCodec.FromStreamEntry(entry);
            var originalMessageId = replayMessage.MessageId ?? entry.Id.ToString();
            replayMessage.MessageId = Guid.NewGuid().ToString("N");
            replayMessage.Headers["x-eventmesh-replay"] = "true";
            replayMessage.Headers["x-eventmesh-original-message-id"] = originalMessageId;

            if (options.Headers is not null)
            {
                foreach (var header in options.Headers)
                {
                    replayMessage.Headers[header.Key] = header.Value;
                }
            }

            await PublishToStreamAsync(destinationStream, replayMessage, cancellationToken);
            replayed++;
        }

        return replayed;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _delayTimer.DisposeAsync();
    }

    private async Task<TransportSendResult> FanOutToTopicAsync(
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        var subscriptions = _topologyManager.GetMatchingSubscriptions(message.Destination, message.RoutingKey);
        if (subscriptions.Count == 0)
        {
            return TransportSendResult.Failure($"No subscriptions matched topic '{message.Destination}'.");
        }

        var messageId = message.MessageId ?? Guid.NewGuid().ToString("N");
        message.MessageId = messageId;

        await PublishToStreamAsync(_topologyManager.ToStreamKey(message.Destination), message, cancellationToken);

        foreach (var subscription in subscriptions)
        {
            var copy = CloneForDelivery(message, Guid.NewGuid().ToString("N"));
            copy.Destination = subscription.Destination;
            await EnqueueToStreamAsync(subscription.DestinationStreamKey, copy, cancellationToken);
        }

        return TransportSendResult.Success(messageId);
    }

    private async Task<TransportSendResult> EnqueueAsync(
        string destination,
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        message.Destination = destination;

        if (message.ScheduledAt is not null && message.ScheduledAt > DateTimeOffset.UtcNow)
        {
            await ScheduleDelayedMessageAsync(destination, message, cancellationToken);
            return TransportSendResult.Success(message.MessageId ?? Guid.NewGuid().ToString("N"));
        }

        var streamKey = ResolveStreamKeyForMessage(destination, message);
        var messageId = await EnqueueToStreamAsync(streamKey, message, cancellationToken);
        return TransportSendResult.Success(messageId, ParseSequenceNumber(messageId));
    }

    private async Task<string> EnqueueToStreamAsync(
        RedisKey streamKey,
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        message.MessageId ??= Guid.NewGuid().ToString("N");
        message.EnqueuedAt ??= DateTimeOffset.UtcNow;
        return await PublishToStreamAsync(streamKey, message, cancellationToken);
    }

    private async Task<string> PublishToStreamAsync(
        RedisKey streamKey,
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        var database = _connection.GetDatabase();
        var entries = RedisStreamsMessageCodec.ToStreamEntries(message);
        var messageId = await database.StreamAddAsync(streamKey, entries).WaitAsync(cancellationToken);
        return messageId.ToString();
    }

    private RedisKey ResolveStreamKeyForMessage(string destination, TransportMessage message)
    {
        if (message.Priority is not null && message.Priority.Value > 0)
        {
            var priority = Math.Clamp(message.Priority.Value, 0, _options.MaxPriority);
            return _topologyManager.ToPriorityStreamKey(destination, priority);
        }

        return _topologyManager.ToStreamKey(destination);
    }

    private async Task ScheduleDelayedMessageAsync(
        string destination,
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        var streamKey = ResolveStreamKeyForMessage(destination, message);
        message.MessageId ??= Guid.NewGuid().ToString("N");
        message.EnqueuedAt ??= DateTimeOffset.UtcNow;

        var database = _connection.GetDatabase();
        var member = RedisStreamsMessageCodec.EncodeDelayedMember(streamKey, message);
        var payloadKey = $"{_options.DelayedMessagesKey}:payload:{member}";
        var hashEntries = RedisStreamsMessageCodec.ToStreamEntries(message)
            .Select(entry => new HashEntry(entry.Name!, entry.Value!))
            .ToArray();

        await database.HashSetAsync(payloadKey, hashEntries).WaitAsync(cancellationToken);
        var score = message.ScheduledAt!.Value.ToUnixTimeMilliseconds();
        await database.SortedSetAddAsync(_options.DelayedMessagesKey, member, score).WaitAsync(cancellationToken);
    }

    private async Task<TransportReceiveResult?> TryReadGroupAsync(
        RedisKey streamKey,
        string consumerGroup,
        string queueName,
        CancellationToken cancellationToken)
    {
        var database = _connection.GetDatabase();

        try
        {
            await EnsureConsumerGroupAsync(streamKey, consumerGroup, cancellationToken);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var entries = await database.StreamReadGroupAsync(
                streamKey,
                consumerGroup,
                _consumerName,
                position: ">",
                count: 1,
                noAck: false)
            .WaitAsync(cancellationToken);

        if (entries.Length == 0)
        {
            return null;
        }

        var entry = entries[0];
        if (IsBootstrapEntry(entry))
        {
            await database.StreamAcknowledgeAsync(streamKey, consumerGroup, entry.Id).WaitAsync(cancellationToken);
            return null;
        }

        return CreateReceiveResult(entry, streamKey, consumerGroup, queueName);
    }

    private static bool IsBootstrapEntry(StreamEntry entry) =>
        entry.Values.Length == 1 && entry.Values[0].Name == "__bootstrap";

    private async Task<TransportReceiveResult?> TryAutoClaimPendingAsync(
        RedisKey streamKey,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var database = _connection.GetDatabase();

        try
        {
            await EnsureConsumerGroupAsync(streamKey, consumerGroup, cancellationToken);
        }
        catch (RedisServerException)
        {
            return null;
        }

        var minIdle = (long)_options.PendingClaimMinIdle.TotalMilliseconds;
        var claimResult = await database.StreamAutoClaimAsync(
                streamKey,
                consumerGroup,
                _consumerName,
                minIdleTimeInMs: minIdle,
                startAtId: "0-0",
                count: 1)
            .WaitAsync(cancellationToken);

        if (claimResult.ClaimedEntries.Length == 0)
        {
            return null;
        }

        var entry = claimResult.ClaimedEntries[0];
        if (IsBootstrapEntry(entry))
        {
            await database.StreamAcknowledgeAsync(streamKey, consumerGroup, entry.Id).WaitAsync(cancellationToken);
            return null;
        }

        var queueName = ResolveQueueNameFromStream(streamKey);
        return CreateReceiveResult(entry, streamKey, consumerGroup, queueName);
    }

    private async Task<TransportReceiveResult?> TryClaimPendingAsync(
        RedisKey streamKey,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var database = _connection.GetDatabase();

        try
        {
            await EnsureConsumerGroupAsync(streamKey, consumerGroup, cancellationToken);
        }
        catch (RedisServerException)
        {
            return null;
        }

        var pendingMessages = await database.StreamPendingMessagesAsync(
                streamKey,
                consumerGroup,
                count: 1,
                consumerName: _consumerName,
                minId: "-",
                maxId: "+")
            .WaitAsync(cancellationToken);

        if (pendingMessages.Length == 0)
        {
            return null;
        }

        var minIdle = (long)_options.PendingClaimMinIdle.TotalMilliseconds;
        var claimedEntries = await database.StreamClaimAsync(
                streamKey,
                consumerGroup,
                _consumerName,
                minIdleTimeInMs: minIdle,
                messageIds: [pendingMessages[0].MessageId])
            .WaitAsync(cancellationToken);

        if (claimedEntries.Length == 0)
        {
            return null;
        }

        var entry = claimedEntries[0];
        if (IsBootstrapEntry(entry))
        {
            await database.StreamAcknowledgeAsync(streamKey, consumerGroup, entry.Id).WaitAsync(cancellationToken);
            return null;
        }

        var queueName = ResolveQueueNameFromStream(streamKey);
        return CreateReceiveResult(entry, streamKey, consumerGroup, queueName);
    }

    private TransportReceiveResult CreateReceiveResult(
        StreamEntry entry,
        RedisKey streamKey,
        string consumerGroup,
        string queueName)
    {
        var message = RedisStreamsMessageCodec.FromStreamEntry(entry);
        message.Destination = queueName;
        var deliveryTag = RedisStreamsMessageCodec.EncodeDeliveryTag(
            streamKey,
            consumerGroup,
            entry.Id,
            _consumerName);

        message.DeliveryTag = deliveryTag;
        return TransportReceiveResult.Received(message, deliveryTag);
    }

    private async Task EnsureConsumerGroupAsync(
        RedisKey streamKey,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var database = _connection.GetDatabase();

        try
        {
            await database.StreamCreateConsumerGroupAsync(streamKey, consumerGroup, position: "0-0", createStream: true)
                .WaitAsync(cancellationToken);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogDebug(
                "Consumer group {ConsumerGroup} already exists on stream {StreamKey}.",
                consumerGroup,
                streamKey);
        }
    }

    private string ResolveQueueNameFromStream(RedisKey streamKey)
    {
        var streamName = streamKey.ToString();
        if (!streamName.StartsWith(_options.StreamPrefix, StringComparison.Ordinal))
        {
            return streamName;
        }

        var remainder = streamName[_options.StreamPrefix.Length..];
        var prioritySeparator = remainder.LastIndexOf(":p", StringComparison.Ordinal);
        return prioritySeparator > 0 ? remainder[..prioritySeparator] : remainder;
    }

    private void PromoteDelayedMessages(object? state)
    {
        if (_disposed == 1)
        {
            return;
        }

        lock (_delayLock)
        {
            try
            {
                PromoteDelayedMessagesCore().GetAwaiter().GetResult();
            }
            catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException or ObjectDisposedException)
            {
                _logger.LogDebug(ex, "Delayed message promotion skipped because Redis is unavailable.");
            }
        }
    }

    private async Task PromoteDelayedMessagesCore()
    {
        var database = _connection.GetDatabase();
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var dueMembers = await database.SortedSetRangeByScoreAsync(
            _options.DelayedMessagesKey,
            start: double.NegativeInfinity,
            stop: now,
            take: 100);

        foreach (var member in dueMembers)
        {
            if (!RedisStreamsMessageCodec.TryDecodeDelayedMember(member!, out var streamKey, out _))
            {
                await database.SortedSetRemoveAsync(_options.DelayedMessagesKey, member);
                continue;
            }

            var removed = await database.SortedSetRemoveAsync(_options.DelayedMessagesKey, member);
            if (!removed)
            {
                continue;
            }

            var payloadKey = $"{_options.DelayedMessagesKey}:payload:{member}";
            var hashEntries = await database.HashGetAllAsync(payloadKey);
            if (hashEntries.Length == 0)
            {
                continue;
            }

            var streamEntries = hashEntries
                .Select(entry => new NameValueEntry(entry.Name!, entry.Value!))
                .ToArray();

            await database.StreamAddAsync(streamKey, streamEntries);
            await database.KeyDeleteAsync(payloadKey);
        }
    }

    private static TransportMessage CloneForDelivery(TransportMessage source, string messageId) => new()
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

    private static long? ParseSequenceNumber(string messageId)
    {
        var separatorIndex = messageId.IndexOf('-', StringComparison.Ordinal);
        if (separatorIndex <= 0)
        {
            return null;
        }

        return long.TryParse(messageId[..separatorIndex], out var sequenceNumber)
            ? sequenceNumber
            : null;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(RedisStreamsBrokerTransport));
        }
    }
}
