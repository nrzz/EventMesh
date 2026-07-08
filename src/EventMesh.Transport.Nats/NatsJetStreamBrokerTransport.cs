using System.Collections.Concurrent;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;

namespace EventMesh.Transport.Nats;

/// <summary>
/// NATS JetStream <see cref="IBrokerTransport"/> with durable consumers, replay, and dead-letter handling.
/// </summary>
public sealed class NatsJetStreamBrokerTransport : IBrokerTransport
{
    private readonly NatsConnectionManager _connectionManager;
    private readonly NatsTopologyManager _topologyManager;
    private readonly NatsTransportOptions _options;
    private readonly ILogger<NatsJetStreamBrokerTransport> _logger;
    private readonly ConcurrentDictionary<string, INatsJSConsumer> _consumers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, InFlightMessage> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<DelayedMessage> _delayedMessages = new();
    private readonly Timer _delayTimer;
    private int _disposed;

    public NatsJetStreamBrokerTransport(
        NatsConnectionManager connectionManager,
        NatsTopologyManager topologyManager,
        IOptions<NatsTransportOptions> options,
        ILogger<NatsJetStreamBrokerTransport> logger)
    {
        _connectionManager = connectionManager;
        _topologyManager = topologyManager;
        _options = options.Value;
        _logger = logger;
        _delayTimer = new Timer(PromoteDelayedMessages, null, _options.DelayCheckInterval, _options.DelayCheckInterval);
    }

    /// <inheritdoc />
    public string Name => "nats";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities() =>
        BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.PubSub
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

        var sequence = await PublishAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);
        return TransportSendResult.Success(message.MessageId, sequence);
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
        var consumer = await GetOrCreateConsumerAsync(receiveTarget, cancellationToken).ConfigureAwait(false);

        while (!cancellationToken.IsCancellationRequested)
        {
            await foreach (var brokerMessage in consumer.FetchAsync<ReadOnlyMemory<byte>>(
                               new NatsJSFetchOpts { MaxMsgs = _options.FetchBatchSize },
                               cancellationToken: cancellationToken)
                           .ConfigureAwait(false))
            {
                if (brokerMessage.Data.IsEmpty)
                {
                    continue;
                }

                var deliveryCount = brokerMessage.Metadata?.NumDelivered ?? 1;
                var transportMessage = NatsMessageCodec.FromNatsMessage(
                    receiveTarget.QueueName,
                    brokerMessage.Data,
                    brokerMessage.Headers,
                    deliveryCount > 0 ? deliveryCount - 1 : 0);

                if (transportMessage.ScheduledAt is not null && transportMessage.ScheduledAt > DateTimeOffset.UtcNow)
                {
                    await brokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var sequence = brokerMessage.Metadata?.Sequence.Stream ?? 0;
                var deliveryTag = NatsMessageCodec.CreateDeliveryTag(
                    receiveTarget.StreamName,
                    receiveTarget.ConsumerName,
                    sequence,
                    brokerMessage.Subject);

                transportMessage.DeliveryTag = deliveryTag;

                _inFlight[deliveryTag] = new InFlightMessage
                {
                    DeliveryTag = deliveryTag,
                    Message = transportMessage,
                    ReceiveTarget = receiveTarget,
                    BrokerMessage = brokerMessage,
                };

                return TransportReceiveResult.Received(transportMessage, deliveryTag);
            }

            await Task.Delay(_options.FetchTimeout, cancellationToken).ConfigureAwait(false);
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

        if (!_inFlight.TryRemove(deliveryTag, out var inFlight))
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is not in flight.");
        }

        await inFlight.BrokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
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

        var nextDeliveryCount = inFlight.Message.DeliveryCount + 1;
        inFlight.Message.DeliveryCount = nextDeliveryCount;

        var maxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts;
        if (_topologyManager.TryGetQueue(inFlight.ReceiveTarget.QueueName, out var queueTopology))
        {
            maxDeliveryAttempts = queueTopology.MaxDeliveryAttempts;
        }

        if (requeue && nextDeliveryCount < maxDeliveryAttempts)
        {
            await inFlight.BrokerMessage.NakAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        var deadLetterDestination = queueTopology?.DeadLetterDestination
            ?? $"{inFlight.ReceiveTarget.QueueName}{_options.DefaultDeadLetterSuffix}";

        inFlight.Message.Headers[NatsMessageCodec.DeadLetterReasonHeader] = requeue
            ? "max-delivery-attempts-exceeded"
            : "rejected-without-requeue";

        await PublishAsync(deadLetterDestination, inFlight.Message, cancellationToken).ConfigureAwait(false);
        await inFlight.BrokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default) =>
        _topologyManager.CreateTopologyAsync(topology, cancellationToken);

    /// <summary>
    /// Replays messages from a JetStream stream by sequence number or timestamp.
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

        var sourceStream = _topologyManager.ToStreamName(options.Source);
        var sourceSubject = _topologyManager.ToSubject(options.Source);
        var destination = options.Destination ?? options.Source;

        var client = await _connectionManager.GetClientAsync(cancellationToken).ConfigureAwait(false);
        var js = client.CreateJetStreamContext();

        var consumerConfig = new ConsumerConfig
        {
            Name = $"replay-{Guid.NewGuid():N}",
            FilterSubject = sourceSubject,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
        };

        if (options.FromOffset.HasValue)
        {
            consumerConfig.DeliverPolicy = ConsumerConfigDeliverPolicy.ByStartSequence;
            consumerConfig.OptStartSeq = (ulong)options.FromOffset.Value;
        }
        else if (options.From.HasValue)
        {
            consumerConfig.DeliverPolicy = ConsumerConfigDeliverPolicy.ByStartTime;
            consumerConfig.OptStartTime = options.From.Value.UtcDateTime;
        }
        else
        {
            consumerConfig.DeliverPolicy = ConsumerConfigDeliverPolicy.All;
        }

        var consumer = await js.CreateConsumerAsync(sourceStream, consumerConfig, cancellationToken).ConfigureAwait(false);

        var replayed = 0L;
        var fetchOpts = new NatsJSFetchOpts
        {
            MaxMsgs = Math.Max(options.BatchSize, 1),
            Expires = TimeSpan.FromSeconds(2),
        };

        while (!cancellationToken.IsCancellationRequested)
        {
            var received = 0;
            await foreach (var brokerMessage in consumer.FetchAsync<ReadOnlyMemory<byte>>(fetchOpts, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                received++;
                if (brokerMessage.Data.IsEmpty)
                {
                    await brokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var sequence = brokerMessage.Metadata?.Sequence.Stream ?? 0;

                if (options.ToOffset is not null && sequence >= (ulong)options.ToOffset.Value)
                {
                    await brokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var message = NatsMessageCodec.FromNatsMessage(
                    options.Source,
                    brokerMessage.Data,
                    brokerMessage.Headers);

                if (options.From is not null)
                {
                    var enqueuedAt = message.EnqueuedAt ?? brokerMessage.Metadata?.Timestamp;
                    if (enqueuedAt is not null && enqueuedAt < options.From)
                    {
                        await brokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                }

                if (options.To is not null)
                {
                    var enqueuedAt = message.EnqueuedAt ?? brokerMessage.Metadata?.Timestamp;
                    if (enqueuedAt is not null && enqueuedAt >= options.To)
                    {
                        await brokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                }

                message.MessageId = Guid.NewGuid().ToString("N");
                message.Headers[NatsMessageCodec.ReplayHeader] = "true";

                if (options.Headers is not null)
                {
                    foreach (var header in options.Headers)
                    {
                        message.Headers[header.Key] = header.Value;
                    }
                }

                await PublishAsync(destination, message, cancellationToken).ConfigureAwait(false);
                await brokerMessage.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                replayed++;

                if (options.MaxMessages is not null && replayed >= options.MaxMessages.Value)
                {
                    return replayed;
                }
            }

            if (received == 0)
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

        await _delayTimer.DisposeAsync().ConfigureAwait(false);
        _consumers.Clear();
        _inFlight.Clear();
    }

    private async Task<TransportSendResult> FanOutToTopicAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        var subscriptions = _topologyManager.GetSubscriptionsForTopic(message.Destination).ToList();
        if (subscriptions.Count == 0)
        {
            var sequence = await PublishAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);
            return TransportSendResult.Success(message.MessageId, sequence);
        }

        await PublishAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);

        var delivered = 0;
        foreach (var subscription in subscriptions)
        {
            if (!MatchesFilter(message.RoutingKey, subscription.Filter))
            {
                continue;
            }

            var copy = CloneForDelivery(message, Guid.NewGuid().ToString("N"));
            await PublishAsync(subscription.Destination, copy, cancellationToken).ConfigureAwait(false);
            delivered++;
        }

        if (delivered == 0)
        {
            return TransportSendResult.Failure($"No subscriptions matched topic '{message.Destination}'.");
        }

        return TransportSendResult.Success(message.MessageId);
    }

    private async Task<long?> PublishAsync(string destination, TransportMessage message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        message.Destination = destination;
        message.MessageId ??= Guid.NewGuid().ToString("N");
        message.EnqueuedAt ??= DateTimeOffset.UtcNow;

        var client = await _connectionManager.GetClientAsync(cancellationToken).ConfigureAwait(false);
        var js = client.CreateJetStreamContext();
        var subject = _topologyManager.ToSubject(destination);
        var headers = NatsMessageCodec.ToNatsHeaders(message);
        var ack = await js.PublishAsync(subject, message.Body.ToArray(), headers: headers, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        ack.EnsureSuccess();
        return ack.Seq is 0 ? null : (long)ack.Seq;
    }

    private async Task<INatsJSConsumer> GetOrCreateConsumerAsync(
        NatsTopologyManager.ReceiveTarget receiveTarget,
        CancellationToken cancellationToken)
    {
        var key = $"{receiveTarget.StreamName}::{receiveTarget.ConsumerName}";
        if (_consumers.TryGetValue(key, out var existing))
        {
            return existing;
        }

        var client = await _connectionManager.GetClientAsync(cancellationToken).ConfigureAwait(false);
        var js = client.CreateJetStreamContext();
        var consumer = await js.GetConsumerAsync(receiveTarget.StreamName, receiveTarget.ConsumerName, cancellationToken)
            .ConfigureAwait(false);

        _consumers[key] = consumer;
        return consumer;
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
                    PublishAsync(delayed.Message.Destination, delayed.Message, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult();
                }
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to promote delayed NATS message {MessageId}.", delayed.Message.MessageId);
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
            throw new ObjectDisposedException(nameof(NatsJetStreamBrokerTransport));
        }
    }

    private sealed class InFlightMessage
    {
        public required string DeliveryTag { get; init; }

        public required TransportMessage Message { get; init; }

        public required NatsTopologyManager.ReceiveTarget ReceiveTarget { get; init; }

        public required INatsJSMsg<ReadOnlyMemory<byte>> BrokerMessage { get; init; }
    }

    private sealed class DelayedMessage(TransportMessage message)
    {
        public TransportMessage Message { get; } = message;
    }
}
