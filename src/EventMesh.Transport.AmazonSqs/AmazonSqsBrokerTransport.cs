using System.Collections.Concurrent;
using Amazon.SQS;
using Amazon.SQS.Model;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AmazonSqs;

/// <summary>
/// Amazon SQS <see cref="IBrokerTransport"/> supporting standard and FIFO queues,
/// native delay delivery, visibility timeouts, and dead-letter redrive policies.
/// </summary>
public sealed class AmazonSqsBrokerTransport : IBrokerTransport
{
    private const int MaxNativeDelaySeconds = 900;

    private readonly IAmazonSQS _sqs;
    private readonly AmazonSqsTopologyManager _topologyManager;
    private readonly AmazonSqsTransportOptions _options;
    private readonly ILogger<AmazonSqsBrokerTransport> _logger;
    private readonly ConcurrentDictionary<string, InFlightMessage> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private int _disposed;

    public AmazonSqsBrokerTransport(
        IAmazonSQS sqs,
        AmazonSqsTopologyManager topologyManager,
        IOptions<AmazonSqsTransportOptions> options,
        ILogger<AmazonSqsBrokerTransport> logger)
    {
        _sqs = sqs;
        _topologyManager = topologyManager;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public string Name => "amazonsqs";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities() =>
        BrokerCapabilities.Fifo
        | BrokerCapabilities.DelayedDelivery
        | BrokerCapabilities.VisibilityTimeout
        | BrokerCapabilities.DeadLettering
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

        if (_topologyManager.IsTopic(message.Destination))
        {
            return await FanOutToTopicAsync(message, cancellationToken).ConfigureAwait(false);
        }

        return await SendToQueueAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);
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
        var queueUrl = await _topologyManager.GetQueueUrlAsync(receiveTarget.QueueName, cancellationToken)
            .ConfigureAwait(false);

        while (!cancellationToken.IsCancellationRequested)
        {
            var response = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = _options.ReceiveWaitTimeSeconds,
                MessageAttributeNames = ["All"],
                MessageSystemAttributeNames = ["All"],
            }, cancellationToken).ConfigureAwait(false);

            if (response.Messages is null || response.Messages.Count == 0)
            {
                await Task.Delay(_options.ReceivePollInterval, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var sqsMessage = response.Messages[0];
            var transportMessage = AmazonSqsMessageCodec.FromSqsMessage(sqsMessage, receiveTarget.QueueName);
            var deliveryTag = Guid.NewGuid().ToString("N");
            transportMessage.DeliveryTag = deliveryTag;

            _topologyManager.TryGetQueue(receiveTarget.QueueName, out var queueTopology);

            _inFlight[deliveryTag] = new InFlightMessage
            {
                DeliveryTag = deliveryTag,
                QueueUrl = queueUrl,
                QueueName = receiveTarget.QueueName,
                ReceiptHandle = sqsMessage.ReceiptHandle,
                Message = transportMessage,
                DeadLetterDestination = queueTopology?.DeadLetterDestination,
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

        await _sqs.DeleteMessageAsync(inFlight.QueueUrl, inFlight.ReceiptHandle, cancellationToken)
            .ConfigureAwait(false);
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

        if (requeue)
        {
            await _sqs.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
            {
                QueueUrl = inFlight.QueueUrl,
                ReceiptHandle = inFlight.ReceiptHandle,
                VisibilityTimeout = 0,
            }, cancellationToken).ConfigureAwait(false);
            return;
        }

        var deadLetterDestination = inFlight.DeadLetterDestination;
        if (!string.IsNullOrWhiteSpace(deadLetterDestination))
        {
            var deadLetterMessage = CloneForDeadLetter(inFlight.Message);
            deadLetterMessage.Destination = deadLetterDestination;
            deadLetterMessage.Headers[AmazonSqsMessageCodec.DeadLetterReasonHeader] = "rejected-without-requeue";

            var publishResult = await SendToQueueAsync(deadLetterDestination, deadLetterMessage, cancellationToken)
                .ConfigureAwait(false);
            if (!publishResult.Succeeded)
            {
                throw new InvalidOperationException(
                    $"Failed to dead-letter message '{deliveryTag}': {publishResult.ErrorMessage}");
            }
        }

        await _sqs.DeleteMessageAsync(inFlight.QueueUrl, inFlight.ReceiptHandle, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default) =>
        _topologyManager.CreateTopologyAsync(topology, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return ValueTask.CompletedTask;
        }

        _inFlight.Clear();
        return ValueTask.CompletedTask;
    }

    private async Task<TransportSendResult> FanOutToTopicAsync(
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        var subscriptions = _topologyManager.GetSubscriptionsForTopic(message.Destination).ToList();
        if (subscriptions.Count == 0)
        {
            return await SendToQueueAsync(message.Destination, message, cancellationToken).ConfigureAwait(false);
        }

        var delivered = 0;
        foreach (var subscription in subscriptions)
        {
            if (!MatchesFilter(message.RoutingKey, subscription.Filter))
            {
                continue;
            }

            var copy = CloneForDelivery(message, Guid.NewGuid().ToString("N"));
            var result = await SendToQueueAsync(subscription.Destination, copy, cancellationToken)
                .ConfigureAwait(false);
            if (!result.Succeeded)
            {
                return result;
            }

            delivered++;
        }

        if (delivered == 0)
        {
            return TransportSendResult.Failure($"No subscriptions matched topic '{message.Destination}'.");
        }

        return TransportSendResult.Success(message.MessageId);
    }

    private async Task<TransportSendResult> SendToQueueAsync(
        string queueName,
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        try
        {
            var queueUrl = await _topologyManager.GetQueueUrlAsync(queueName, cancellationToken).ConfigureAwait(false);
            var fifo = _topologyManager.IsFifoQueue(queueName);
            var delaySeconds = ResolveDelaySeconds(message.ScheduledAt);
            var request = AmazonSqsMessageCodec.ToSendRequest(queueUrl, message, fifo, delaySeconds);

            var response = await _sqs.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
            return TransportSendResult.Success(response.MessageId ?? message.MessageId);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Failed to send Amazon SQS message to {Destination}.", queueName);
            return TransportSendResult.Failure(exception.Message);
        }
    }

    private static int? ResolveDelaySeconds(DateTimeOffset? scheduledAt)
    {
        if (scheduledAt is null)
        {
            return null;
        }

        var delay = scheduledAt.Value - DateTimeOffset.UtcNow;
        if (delay <= TimeSpan.Zero)
        {
            return null;
        }

        return (int)Math.Min(MaxNativeDelaySeconds, Math.Ceiling(delay.TotalSeconds));
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

    private static TransportMessage CloneForDeadLetter(TransportMessage source) => new()
    {
        MessageId = Guid.NewGuid().ToString("N"),
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
            throw new ObjectDisposedException(nameof(AmazonSqsBrokerTransport));
        }
    }

    private sealed class InFlightMessage
    {
        public required string DeliveryTag { get; init; }

        public required string QueueUrl { get; init; }

        public required string QueueName { get; init; }

        public required string ReceiptHandle { get; init; }

        public required TransportMessage Message { get; init; }

        public string? DeadLetterDestination { get; init; }
    }
}
