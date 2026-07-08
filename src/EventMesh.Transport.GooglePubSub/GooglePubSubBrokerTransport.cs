using System.Collections.Concurrent;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.GooglePubSub;

/// <summary>
/// Google Pub/Sub <see cref="IBrokerTransport"/> implementation.
/// </summary>
public sealed class GooglePubSubBrokerTransport : IBrokerTransport
{
    private readonly GooglePubSubTopologyManager _topologyManager;
    private readonly GooglePubSubTransportOptions _options;
    private readonly ILogger<GooglePubSubBrokerTransport> _logger;
    private readonly ConcurrentDictionary<string, PublisherClient> _publishers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, InFlightMessage> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<DelayedMessage> _delayedMessages = new();
    private readonly Timer _delayTimer;
    private readonly SemaphoreSlim _publisherLock = new(1, 1);
    private int _disposed;

    public GooglePubSubBrokerTransport(
        GooglePubSubTopologyManager topologyManager,
        IOptions<GooglePubSubTransportOptions> options,
        ILogger<GooglePubSubBrokerTransport> logger)
    {
        _topologyManager = topologyManager;
        _options = options.Value;
        _logger = logger;
        _delayTimer = new Timer(PromoteDelayedMessages, null, _options.DelayCheckInterval, _options.DelayCheckInterval);
    }

    /// <inheritdoc />
    public string Name => "googlepubsub";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities() =>
        BrokerCapabilities.Ordering
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.TopologyProvisioning;

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

        message.MessageId ??= Guid.NewGuid().ToString("N");
        message.EnqueuedAt ??= DateTimeOffset.UtcNow;

        if (message.ScheduledAt is not null && message.ScheduledAt > DateTimeOffset.UtcNow)
        {
            _delayedMessages.Enqueue(new DelayedMessage(message));
            return TransportSendResult.Success(message.MessageId);
        }

        await PublishAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);
        return TransportSendResult.Success(message.MessageId);
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
        var subscriberApi = await _topologyManager.GetSubscriberApiClientAsync(cancellationToken).ConfigureAwait(false);
        var subscriptionName = _topologyManager.GetSubscriptionName(receiveTarget.SubscriptionId);

        while (!cancellationToken.IsCancellationRequested)
        {
            PullResponse pullResponse;
            try
            {
                pullResponse = await subscriberApi.PullAsync(
                    new PullRequest
                    {
                        SubscriptionAsSubscriptionName = subscriptionName,
                        MaxMessages = 1,
                    },
                    cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound)
            {
                throw new InvalidOperationException(
                    $"Pub/Sub subscription '{receiveTarget.SubscriptionId}' was not found.",
                    exception);
            }

            if (pullResponse.ReceivedMessages.Count == 0)
            {
                await Task.Delay(_options.ReceivePollInterval, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var received = pullResponse.ReceivedMessages[0];
            var transportMessage = GooglePubSubMessageCodec.FromPubsubMessage(
                receiveTarget.LogicalDestination,
                received.Message);

            if (transportMessage.ScheduledAt is not null && transportMessage.ScheduledAt > DateTimeOffset.UtcNow)
            {
                await subscriberApi.ModifyAckDeadlineAsync(
                    subscriptionName,
                    [received.AckId],
                    0,
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            var deliveryTag = GooglePubSubMessageCodec.CreateDeliveryTag(
                receiveTarget.SubscriptionId,
                received.AckId,
                received.Message.MessageId);

            transportMessage.DeliveryTag = deliveryTag;

            _inFlight[deliveryTag] = new InFlightMessage
            {
                DeliveryTag = deliveryTag,
                SubscriptionId = receiveTarget.SubscriptionId,
                AckId = received.AckId,
                Message = transportMessage,
                ReceiveTarget = receiveTarget,
            };

            return TransportReceiveResult.Received(transportMessage, deliveryTag);
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

        var subscriberApi = await _topologyManager.GetSubscriberApiClientAsync(cancellationToken).ConfigureAwait(false);
        var subscriptionName = _topologyManager.GetSubscriptionName(inFlight.SubscriptionId);
        await subscriberApi.AcknowledgeAsync(subscriptionName, [inFlight.AckId], cancellationToken).ConfigureAwait(false);
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

        var subscriberApi = await _topologyManager.GetSubscriberApiClientAsync(cancellationToken).ConfigureAwait(false);
        var subscriptionName = _topologyManager.GetSubscriptionName(inFlight.SubscriptionId);
        var nextDeliveryCount = inFlight.Message.DeliveryCount + 1;
        inFlight.Message.DeliveryCount = nextDeliveryCount;

        var maxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts;
        if (_topologyManager.TryGetQueue(inFlight.ReceiveTarget.LogicalDestination, out var queueTopology))
        {
            maxDeliveryAttempts = queueTopology.MaxDeliveryAttempts;
        }

        if (requeue && nextDeliveryCount < maxDeliveryAttempts)
        {
            await PublishAsync(inFlight.ReceiveTarget.TopicId, inFlight.Message, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var deadLetterDestination = queueTopology?.DeadLetterDestination
                ?? $"{inFlight.ReceiveTarget.LogicalDestination}{_options.DefaultDeadLetterSuffix}";

            inFlight.Message.Headers[GooglePubSubMessageCodec.DeadLetterReasonHeader] = requeue
                ? "max-delivery-attempts-exceeded"
                : "rejected-without-requeue";

            await PublishAsync(deadLetterDestination, inFlight.Message, cancellationToken).ConfigureAwait(false);
        }

        await subscriberApi.AcknowledgeAsync(subscriptionName, [inFlight.AckId], cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default) =>
        _topologyManager.CreateTopologyAsync(topology, cancellationToken);

    /// <summary>
    /// Replays messages from a subscription using seek and republication.
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

        var subscriptionId = _topologyManager.ResolveReplaySubscriptionId(options.Source);
        var destination = options.Destination ?? options.Source;
        var subscriberApi = await _topologyManager.GetSubscriberApiClientAsync(cancellationToken).ConfigureAwait(false);
        var subscriptionName = _topologyManager.GetSubscriptionName(subscriptionId);

        var seekTime = options.From ?? DateTimeOffset.UnixEpoch;
        await subscriberApi.SeekAsync(
            new SeekRequest
            {
                SubscriptionAsSubscriptionName = subscriptionName,
                Time = Timestamp.FromDateTimeOffset(seekTime),
            },
            cancellationToken).ConfigureAwait(false);

        var replayed = 0L;
        var emptyPulls = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var pullResponse = await subscriberApi.PullAsync(
                new PullRequest
                {
                    SubscriptionAsSubscriptionName = subscriptionName,
                    MaxMessages = Math.Max(1, options.BatchSize),
                },
                cancellationToken).ConfigureAwait(false);

            if (pullResponse.ReceivedMessages.Count == 0)
            {
                emptyPulls++;
                if (emptyPulls >= 3)
                {
                    break;
                }

                await Task.Delay(_options.ReceivePollInterval, cancellationToken).ConfigureAwait(false);
                continue;
            }

            emptyPulls = 0;
            var ackIds = new List<string>(pullResponse.ReceivedMessages.Count);

            foreach (var received in pullResponse.ReceivedMessages)
            {
                if (options.To is not null && received.Message.PublishTime.ToDateTimeOffset() >= options.To.Value)
                {
                    ackIds.Add(received.AckId);
                    continue;
                }

                var message = GooglePubSubMessageCodec.FromPubsubMessage(destination, received.Message);
                message.MessageId = Guid.NewGuid().ToString("N");
                message.Headers[GooglePubSubMessageCodec.ReplayHeader] = "true";

                if (options.Headers is not null)
                {
                    foreach (var header in options.Headers)
                    {
                        message.Headers[header.Key] = header.Value;
                    }
                }

                await PublishAsync(destination, message, cancellationToken).ConfigureAwait(false);
                replayed++;
                ackIds.Add(received.AckId);

                if (options.MaxMessages is not null && replayed >= options.MaxMessages.Value)
                {
                    break;
                }
            }

            if (ackIds.Count > 0)
            {
                await subscriberApi.AcknowledgeAsync(subscriptionName, ackIds, cancellationToken).ConfigureAwait(false);
            }

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

        foreach (var publisher in _publishers.Values)
        {
            try
            {
                await publisher.ShutdownAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.LogDebug(exception, "Failed to shut down Pub/Sub publisher.");
            }
        }

        _publishers.Clear();
        _publisherLock.Dispose();
        await Task.CompletedTask;
    }

    private async Task PublishAsync(string topicId, TransportMessage message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var publisher = await GetOrCreatePublisherAsync(topicId, cancellationToken).ConfigureAwait(false);
        var pubsubMessage = GooglePubSubMessageCodec.ToPubsubMessage(message);

        try
        {
            await publisher.PublishAsync(pubsubMessage).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Pub/Sub publish to {Topic} failed.", topicId);
            throw;
        }
    }

    private async Task<PublisherClient> GetOrCreatePublisherAsync(string topicId, CancellationToken cancellationToken)
    {
        if (_publishers.TryGetValue(topicId, out var existingPublisher))
        {
            return existingPublisher;
        }

        await _publisherLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_publishers.TryGetValue(topicId, out existingPublisher))
            {
                return existingPublisher;
            }

            var topicName = _topologyManager.GetTopicName(topicId);
            var publisher = await new PublisherClientBuilder
            {
                TopicName = topicName,
                EmulatorDetection = _options.EmulatorDetection,
            }.BuildAsync(cancellationToken).ConfigureAwait(false);

            _publishers[topicId] = publisher;
            return publisher;
        }
        finally
        {
            _publisherLock.Release();
        }
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
                PublishAsync(delayed.Message.Destination, delayed.Message, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to promote delayed Pub/Sub message {MessageId}.", delayed.Message.MessageId);
                pending.Add(delayed);
            }
        }

        foreach (var delayed in pending)
        {
            _delayedMessages.Enqueue(delayed);
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(GooglePubSubBrokerTransport));
        }
    }

    private sealed class InFlightMessage
    {
        public required string DeliveryTag { get; init; }

        public required string SubscriptionId { get; init; }

        public required string AckId { get; init; }

        public required TransportMessage Message { get; init; }

        public required GooglePubSubTopologyManager.ReceiveTarget ReceiveTarget { get; init; }
    }

    private sealed class DelayedMessage(TransportMessage message)
    {
        public TransportMessage Message { get; } = message;
    }
}
