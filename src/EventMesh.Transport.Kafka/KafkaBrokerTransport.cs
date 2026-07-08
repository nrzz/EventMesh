using System.Collections.Concurrent;
using Confluent.Kafka;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.Kafka;

/// <summary>
/// Kafka <see cref="IBrokerTransport"/> implementation using Confluent.Kafka.
/// </summary>
public sealed class KafkaBrokerTransport : IBrokerTransport
{
    private readonly KafkaTopologyManager _topologyManager;
    private readonly KafkaTransportOptions _options;
    private readonly ILogger<KafkaBrokerTransport> _logger;
    private readonly ConcurrentDictionary<string, KafkaConsumerSession> _consumerSessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, InFlightMessage> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<DelayedMessage> _delayedMessages = new();
    private readonly Timer _delayTimer;
    private readonly object _producerLock = new();
    private IProducer<string, byte[]>? _producer;
    private int _disposed;

    public KafkaBrokerTransport(
        KafkaTopologyManager topologyManager,
        IOptions<KafkaTransportOptions> options,
        ILogger<KafkaBrokerTransport> logger)
    {
        _topologyManager = topologyManager;
        _options = options.Value;
        _logger = logger;
        _delayTimer = new Timer(PromoteDelayedMessages, null, _options.DelayCheckInterval, _options.DelayCheckInterval);
    }

    /// <inheritdoc />
    public string Name => "kafka";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities() =>
        BrokerCapabilities.Partitions
        | BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.Replay
        | BrokerCapabilities.Ordering
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.TopologyProvisioning;

    /// <inheritdoc />
    public async Task<TransportSendResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(message);

        if (string.IsNullOrWhiteSpace(message.Destination))
        {
            return TransportSendResult.Failure("Destination is required.");
        }

        message.MessageId ??= Guid.NewGuid().ToString("N");
        message.EnqueuedAt ??= DateTimeOffset.UtcNow;

        if (message.ScheduledAt is not null && message.ScheduledAt > DateTimeOffset.UtcNow)
        {
            _delayedMessages.Enqueue(new DelayedMessage(message));
            return TransportSendResult.Success(message.MessageId);
        }

        if (_topologyManager.TryGetTopic(message.Destination, out _))
        {
            return await FanOutToTopicAsync(message, cancellationToken).ConfigureAwait(false);
        }

        await ProduceAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);
        return TransportSendResult.Success(message.MessageId);
    }

    /// <inheritdoc />
    public Task<TransportReceiveResult> ReceiveAsync(
        string queueOrSubscription,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(queueOrSubscription);

        var receiveTarget = _topologyManager.ResolveReceiveTarget(queueOrSubscription);
        var session = GetOrCreateConsumerSession(receiveTarget);

        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = session.Consumer.Consume(_options.ConsumerPollInterval);
            if (consumeResult is null)
            {
                continue;
            }

            if (consumeResult.IsPartitionEOF || consumeResult.Message.Value is null)
            {
                continue;
            }

            var transportMessage = KafkaMessageCodec.FromKafkaRecord(
                receiveTarget.Topic,
                consumeResult.Message.Value,
                consumeResult.Message.Headers);

            if (transportMessage.ScheduledAt is not null && transportMessage.ScheduledAt > DateTimeOffset.UtcNow)
            {
                continue;
            }

            var deliveryTag = KafkaMessageCodec.CreateDeliveryTag(
                consumeResult.Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value,
                receiveTarget.ConsumerGroup);

            transportMessage.DeliveryTag = deliveryTag;

            _inFlight[deliveryTag] = new InFlightMessage
            {
                DeliveryTag = deliveryTag,
                Message = transportMessage,
                ReceiveTarget = receiveTarget,
                TopicPartitionOffset = new TopicPartitionOffset(
                    consumeResult.TopicPartition,
                    consumeResult.Offset.Value),
            };

            return Task.FromResult(TransportReceiveResult.Received(transportMessage, deliveryTag));
        }

        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(TransportReceiveResult.Empty());
    }

    /// <inheritdoc />
    public Task AcknowledgeAsync(string deliveryTag, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(deliveryTag);

        if (!_inFlight.TryRemove(deliveryTag, out var inFlight))
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is not in flight.");
        }

        var session = GetOrCreateConsumerSession(inFlight.ReceiveTarget);
        session.Consumer.Commit([inFlight.TopicPartitionOffset]);
        return Task.CompletedTask;
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

        if (!_inFlight.TryRemove(deliveryTag, out var inFlight))
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is not in flight.");
        }

        var session = GetOrCreateConsumerSession(inFlight.ReceiveTarget);
        var nextDeliveryCount = inFlight.Message.DeliveryCount + 1;
        inFlight.Message.DeliveryCount = nextDeliveryCount;

        var maxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts;
        if (_topologyManager.TryGetQueue(inFlight.ReceiveTarget.Topic, out var queueTopology))
        {
            maxDeliveryAttempts = queueTopology.MaxDeliveryAttempts;
        }

        if (requeue && nextDeliveryCount < maxDeliveryAttempts)
        {
            await ProduceAsync(inFlight.ReceiveTarget.Topic, inFlight.Message, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var deadLetterDestination = queueTopology?.DeadLetterDestination
                ?? $"{inFlight.ReceiveTarget.Topic}{_options.DefaultDeadLetterSuffix}";

            inFlight.Message.Headers[KafkaMessageCodec.DeadLetterReasonHeader] = requeue
                ? "max-delivery-attempts-exceeded"
                : "rejected-without-requeue";

            await ProduceAsync(deadLetterDestination, inFlight.Message, cancellationToken).ConfigureAwait(false);
        }

        session.Consumer.Commit([inFlight.TopicPartitionOffset]);
    }

    /// <inheritdoc />
    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default) =>
        _topologyManager.CreateTopologyAsync(topology, cancellationToken);

    /// <summary>
    /// Replays messages from a Kafka topic using partition seek and republication.
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

        var sourceTopic = options.Source;
        var destinationTopic = options.Destination ?? sourceTopic;
        var consumerGroup = $"{_options.GroupId}-replay-{Guid.NewGuid():N}";

        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = consumerGroup,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();

        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _options.BootstrapServers,
        }).Build();

        var metadata = admin.GetMetadata(sourceTopic, TimeSpan.FromSeconds(10));
        var topicMetadata = metadata.Topics.FirstOrDefault(topic =>
            string.Equals(topic.Topic, sourceTopic, StringComparison.OrdinalIgnoreCase));

        if (topicMetadata is null || topicMetadata.Partitions.Count == 0)
        {
            return 0;
        }

        var partitions = topicMetadata.Partitions
            .Select(partition => new TopicPartition(sourceTopic, partition.PartitionId))
            .ToList();

        consumer.Assign(partitions);

        foreach (var partition in partitions)
        {
            var offsets = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
            var seekOffset = options.FromOffset ?? offsets.Low.Value;
            consumer.Seek(new TopicPartitionOffset(partition, new Offset(seekOffset)));
        }

        var replayed = 0L;
        var endOffsets = partitions.ToDictionary(
            partition => partition,
            partition => consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High.Value);

        while (!cancellationToken.IsCancellationRequested)
        {
            var allPartitionsAtEnd = partitions.All(partition =>
            {
                var position = consumer.Position(partition);
                return position.Value >= endOffsets[partition];
            });

            if (allPartitionsAtEnd)
            {
                break;
            }

            var result = consumer.Consume(TimeSpan.FromMilliseconds(250));
            if (result is null || result.IsPartitionEOF || result.Message.Value is null)
            {
                continue;
            }

            if (options.From is not null)
            {
                var timestamp = result.Message.Timestamp.UnixTimestampMs;
                var messageTime = DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
                if (messageTime < options.From.Value)
                {
                    continue;
                }
            }

            if (options.To is not null)
            {
                var timestamp = result.Message.Timestamp.UnixTimestampMs;
                var messageTime = DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
                if (messageTime >= options.To.Value)
                {
                    continue;
                }
            }

            if (options.ToOffset is not null && result.Offset.Value >= options.ToOffset.Value)
            {
                continue;
            }

            var message = KafkaMessageCodec.FromKafkaRecord(sourceTopic, result.Message.Value, result.Message.Headers);
            message.MessageId = Guid.NewGuid().ToString("N");
            message.Headers[KafkaMessageCodec.ReplayHeader] = "true";

            if (options.Headers is not null)
            {
                foreach (var header in options.Headers)
                {
                    message.Headers[header.Key] = header.Value;
                }
            }

            await ProduceAsync(destinationTopic, message, cancellationToken).ConfigureAwait(false);
            replayed++;

            if (options.MaxMessages is not null && replayed >= options.MaxMessages.Value)
            {
                break;
            }
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

        _delayTimer.Dispose();

        foreach (var session in _consumerSessions.Values)
        {
            session.Consumer.Dispose();
        }

        _consumerSessions.Clear();

        lock (_producerLock)
        {
            _producer?.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
            _producer = null;
        }

        await Task.CompletedTask;
    }

    private async Task<TransportSendResult> FanOutToTopicAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        var subscriptions = _topologyManager.GetSubscriptionsForTopic(message.Destination).ToList();
        if (subscriptions.Count == 0)
        {
            await ProduceAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);
            return TransportSendResult.Success(message.MessageId);
        }

        var delivered = 0;
        foreach (var subscription in subscriptions)
        {
            if (!MatchesFilter(message.RoutingKey, subscription.Filter))
            {
                continue;
            }

            var copy = CloneForDelivery(message, Guid.NewGuid().ToString("N"));
            await ProduceAsync(subscription.Destination, copy, cancellationToken).ConfigureAwait(false);
            delivered++;
        }

        if (delivered == 0)
        {
            return TransportSendResult.Failure($"No subscriptions matched topic '{message.Destination}'.");
        }

        return TransportSendResult.Success(message.MessageId);
    }

    private Task ProduceAsync(string topic, TransportMessage message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var producer = GetProducer();
        var key = message.PartitionKey ?? message.SessionId ?? message.MessageId ?? string.Empty;
        var kafkaMessage = new Message<string, byte[]>
        {
            Key = key,
            Value = message.Body.ToArray(),
            Headers = KafkaMessageCodec.ToKafkaHeaders(message),
        };

        try
        {
            producer.Produce(
                topic,
                kafkaMessage,
                report =>
                {
                    if (report.Error.IsError)
                    {
                        _logger.LogError(
                            "Kafka produce to {Topic} failed: {Reason}",
                            topic,
                            report.Error.Reason);
                    }
                });
        }
        catch (ProduceException<string, byte[]> exception)
        {
            _logger.LogError(exception, "Kafka produce to {Topic} failed.", topic);
            throw;
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        return Task.CompletedTask;
    }

    private IProducer<string, byte[]> GetProducer()
    {
        lock (_producerLock)
        {
            if (_producer is not null)
            {
                return _producer;
            }

            _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                Acks = _options.Acks,
                CompressionType = _options.CompressionType,
                EnableIdempotence = true,
            }).Build();

            return _producer;
        }
    }

    private KafkaConsumerSession GetOrCreateConsumerSession(KafkaTopologyManager.ReceiveTarget receiveTarget)
    {
        var sessionKey = $"{receiveTarget.Topic}::{receiveTarget.ConsumerGroup}";
        return _consumerSessions.GetOrAdd(sessionKey, _ =>
        {
            var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = receiveTarget.ConsumerGroup,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            }).Build();

            consumer.Subscribe(receiveTarget.Topic);
            return new KafkaConsumerSession(consumer);
        });
    }

    private void PromoteDelayedMessages(object? state)
    {
        var now = DateTimeOffset.UtcNow;
        var pending = new List<DelayedMessage>();

        while (_delayedMessages.TryDequeue(out var delayed))
        {
            if (delayed.Message.ScheduledAt is not null && delayed.Message.ScheduledAt > now)
            {
                pending.Add(delayed);
                continue;
            }

            try
            {
                if (_topologyManager.TryGetTopic(delayed.Message.Destination, out _))
                {
                    FanOutToTopicAsync(delayed.Message, CancellationToken.None).GetAwaiter().GetResult();
                }
                else
                {
                    ProduceAsync(delayed.Message.Destination, delayed.Message, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult();
                }
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to promote delayed Kafka message {MessageId}.", delayed.Message.MessageId);
                pending.Add(delayed);
            }
        }

        foreach (var delayed in pending)
        {
            _delayedMessages.Enqueue(delayed);
        }
    }

    private static bool MatchesFilter(string? routingKey, string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter) || filter is "#" or "*")
        {
            return true;
        }

        return string.Equals(routingKey, filter, StringComparison.OrdinalIgnoreCase);
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

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(KafkaBrokerTransport));
        }
    }

    private sealed class KafkaConsumerSession(IConsumer<string, byte[]> consumer)
    {
        public IConsumer<string, byte[]> Consumer { get; } = consumer;
    }

    private sealed class InFlightMessage
    {
        public required string DeliveryTag { get; init; }

        public required TransportMessage Message { get; init; }

        public required KafkaTopologyManager.ReceiveTarget ReceiveTarget { get; init; }

        public required TopicPartitionOffset TopicPartitionOffset { get; init; }
    }

    private sealed class DelayedMessage(TransportMessage message)
    {
        public TransportMessage Message { get; } = message;
    }
}
