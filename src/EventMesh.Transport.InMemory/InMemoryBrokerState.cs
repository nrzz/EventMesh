using System.Collections.Concurrent;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.InMemory;

/// <summary>
/// Shared broker state backing all in-memory transport instances.
/// </summary>
public sealed class InMemoryBrokerState : IDisposable
{
    private readonly InMemoryMessageStore _messageStore;
    private readonly InMemoryTransportOptions _options;
    private readonly ConcurrentDictionary<string, DestinationQueue> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TopicState> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SubscriptionState> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, InFlightMessage> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _delayLock = new();
    private readonly Timer _delayTimer;
    private int _disposed;

    public InMemoryBrokerState(InMemoryMessageStore messageStore, IOptions<InMemoryTransportOptions> options)
    {
        _messageStore = messageStore;
        _options = options.Value;
        _delayTimer = new Timer(PromoteDelayedMessages, null, _options.DelayCheckInterval, _options.DelayCheckInterval);
    }

    public void CreateTopology(TopologyDefinition topology)
    {
        ArgumentNullException.ThrowIfNull(topology);

        if (topology.ReplaceExisting)
        {
            _queues.Clear();
            _topics.Clear();
            _subscriptions.Clear();
            _inFlight.Clear();
        }

        foreach (var queue in topology.Queues)
        {
            var destinationQueue = GetOrCreateQueue(queue.Name);
            destinationQueue.DeadLetterDestination = queue.DeadLetterDestination
                ?? $"{queue.Name}{_options.DefaultDeadLetterSuffix}";
            destinationQueue.MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts;
            destinationQueue.Fifo = queue.Fifo;
        }

        foreach (var topic in topology.Topics)
        {
            _topics[topic.Name] = new TopicState
            {
                Name = topic.Name,
                Ordered = topic.Ordered,
            };
            GetOrCreateQueue(topic.Name);
        }

        foreach (var subscription in topology.Subscriptions)
        {
            var destination = ResolveSubscriptionDestination(subscription);
            GetOrCreateQueue(destination);

            _subscriptions[subscription.Name] = new SubscriptionState
            {
                Name = subscription.Name,
                Topic = subscription.Topic,
                Destination = destination,
                Filter = subscription.Filter,
                ConsumerGroup = subscription.ConsumerGroup,
            };

            if (_topics.TryGetValue(subscription.Topic, out var topic))
            {
                topic.Subscriptions.Add(subscription.Name);
            }
        }
    }

    public TransportSendResult Enqueue(TransportMessage message, bool recordInStore = true)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.Destination);

        var messageId = message.MessageId ?? Guid.NewGuid().ToString("N");
        message.MessageId = messageId;
        message.EnqueuedAt ??= DateTimeOffset.UtcNow;

        if (recordInStore)
        {
            _messageStore.Record(message, message.Destination);
        }

        if (_topics.ContainsKey(message.Destination))
        {
            return FanOutToTopic(message);
        }

        Enqueue(message.Destination, message, recordInStore: false);
        return TransportSendResult.Success(messageId);
    }

    public void Enqueue(string destination, TransportMessage message, bool recordInStore)
    {
        if (recordInStore)
        {
            _messageStore.Record(message, destination);
        }

        var queue = GetOrCreateQueue(destination);
        queue.Enqueue(message, _options.MaxPriority);
    }

    public bool TryDequeue(string queueOrSubscription, out TransportMessage message, out string deliveryTag)
    {
        message = null!;
        deliveryTag = string.Empty;

        var receiveTarget = ResolveReceiveTarget(queueOrSubscription);
        if (!_queues.TryGetValue(receiveTarget, out var queue))
        {
            return false;
        }

        if (!queue.TryDequeue(out var queuedMessage))
        {
            return false;
        }

        deliveryTag = Guid.NewGuid().ToString("N");
        message = queuedMessage;
        message.DeliveryTag = deliveryTag;

        _inFlight[deliveryTag] = new InFlightMessage
        {
            DeliveryTag = deliveryTag,
            Message = queuedMessage,
            QueueName = receiveTarget,
        };

        return true;
    }

    public bool TryAcknowledge(string deliveryTag)
    {
        return _inFlight.TryRemove(deliveryTag, out _);
    }

    public bool TryReject(string deliveryTag, bool requeue, out TransportMessage? rejectedMessage)
    {
        rejectedMessage = null;

        if (!_inFlight.TryRemove(deliveryTag, out var inFlight))
        {
            return false;
        }

        rejectedMessage = inFlight.Message;
        var queue = GetOrCreateQueue(inFlight.QueueName);
        var nextDeliveryCount = inFlight.Message.DeliveryCount + 1;
        inFlight.Message.DeliveryCount = nextDeliveryCount;

        if (requeue && nextDeliveryCount < queue.MaxDeliveryAttempts)
        {
            queue.Enqueue(inFlight.Message, _options.MaxPriority);
            return true;
        }

        var deadLetterDestination = queue.DeadLetterDestination
            ?? $"{inFlight.QueueName}{_options.DefaultDeadLetterSuffix}";

        inFlight.Message.Headers["x-eventmesh-dead-letter-reason"] = requeue
            ? "max-delivery-attempts-exceeded"
            : "rejected-without-requeue";

        GetOrCreateQueue(deadLetterDestination).Enqueue(inFlight.Message, _options.MaxPriority);
        return true;
    }

    /// <summary>
    /// Gets the names of all provisioned queues.
    /// </summary>
    public IReadOnlyList<string> GetQueueNames() =>
        _queues.Keys.OrderBy(name => name, StringComparer.OrdinalIgnoreCase).ToArray();

    /// <summary>
    /// Gets the names of all provisioned topics.
    /// </summary>
    public IReadOnlyList<string> GetTopicNames() =>
        _topics.Keys.OrderBy(name => name, StringComparer.OrdinalIgnoreCase).ToArray();

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _delayTimer.Dispose();
    }

    private TransportSendResult FanOutToTopic(TransportMessage message)
    {
        if (!_topics.TryGetValue(message.Destination, out var topic))
        {
            return TransportSendResult.Failure($"Topic '{message.Destination}' does not exist.");
        }

        var delivered = 0;
        foreach (var subscriptionName in topic.Subscriptions)
        {
            if (!_subscriptions.TryGetValue(subscriptionName, out var subscription))
            {
                continue;
            }

            if (!MatchesFilter(message.RoutingKey, subscription.Filter))
            {
                continue;
            }

            var copy = CloneForDelivery(message, Guid.NewGuid().ToString("N"));
            Enqueue(subscription.Destination, copy, recordInStore: false);
            delivered++;
        }

        if (delivered == 0)
        {
            return TransportSendResult.Failure($"No subscriptions matched topic '{message.Destination}'.");
        }

        return TransportSendResult.Success(message.MessageId);
    }

    private string ResolveReceiveTarget(string queueOrSubscription)
    {
        if (_subscriptions.TryGetValue(queueOrSubscription, out var subscription))
        {
            return subscription.Destination;
        }

        return queueOrSubscription;
    }

    private static string ResolveSubscriptionDestination(SubscriptionDefinition subscription)
    {
        if (!string.IsNullOrWhiteSpace(subscription.ConsumerGroup))
        {
            var baseDestination = subscription.Destination ?? subscription.Name;
            return $"{baseDestination}::{subscription.ConsumerGroup}";
        }

        return subscription.Destination ?? subscription.Name;
    }

    private DestinationQueue GetOrCreateQueue(string name) =>
        _queues.GetOrAdd(name, static _ => new DestinationQueue());

    private void PromoteDelayedMessages(object? state)
    {
        lock (_delayLock)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var queue in _queues.Values)
            {
                queue.PromoteDelayedMessages(now);
            }
        }
    }

    private static bool MatchesFilter(string? routingKey, string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter) || filter == "#" || filter == "*")
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

    private sealed class TopicState
    {
        public required string Name { get; init; }

        public bool Ordered { get; init; }

        public List<string> Subscriptions { get; } = [];
    }

    private sealed class SubscriptionState
    {
        public required string Name { get; init; }

        public required string Topic { get; init; }

        public required string Destination { get; init; }

        public string? Filter { get; init; }

        public string? ConsumerGroup { get; init; }
    }

    private sealed class InFlightMessage
    {
        public required string DeliveryTag { get; init; }

        public required TransportMessage Message { get; init; }

        public required string QueueName { get; init; }
    }

    private sealed class DestinationQueue
    {
        private readonly PriorityQueue<TransportMessage, (int PriorityKey, long Sequence)> _ready = new();
        private readonly List<TransportMessage> _delayed = [];
        private readonly object _lock = new();
        private long _sequence;
        private int _maxPriority = 10;

        public string? DeadLetterDestination { get; set; }

        public int MaxDeliveryAttempts { get; set; } = 5;

        public bool Fifo { get; set; }

        public void Enqueue(TransportMessage message, int maxPriority)
        {
            lock (_lock)
            {
                _maxPriority = maxPriority;
                var priority = NormalizePriority(message.Priority, maxPriority);
                var sequence = Interlocked.Increment(ref _sequence);

                if (message.ScheduledAt is not null && message.ScheduledAt > DateTimeOffset.UtcNow)
                {
                    _delayed.Add(message);
                    return;
                }

                if (message.TimeToLive is not null && message.EnqueuedAt is not null
                    && message.EnqueuedAt.Value.Add(message.TimeToLive.Value) <= DateTimeOffset.UtcNow)
                {
                    return;
                }

                _ready.Enqueue(message, (CreatePriorityKey(priority, maxPriority), sequence));
            }
        }

        public bool TryDequeue(out TransportMessage message)
        {
            lock (_lock)
            {
                PromoteDelayedMessagesLocked(DateTimeOffset.UtcNow);

                if (_ready.TryDequeue(out var dequeued, out _))
                {
                    message = dequeued;
                    return true;
                }

                message = null!;
                return false;
            }
        }

        public void PromoteDelayedMessages(DateTimeOffset now)
        {
            lock (_lock)
            {
                PromoteDelayedMessagesLocked(now);
            }
        }

        private void PromoteDelayedMessagesLocked(DateTimeOffset now)
        {
            for (var index = _delayed.Count - 1; index >= 0; index--)
            {
                var message = _delayed[index];
                if (message.ScheduledAt is null || message.ScheduledAt > now)
                {
                    continue;
                }

                _delayed.RemoveAt(index);
                var priority = NormalizePriority(message.Priority, _maxPriority);
                var sequence = Interlocked.Increment(ref _sequence);
                _ready.Enqueue(message, (CreatePriorityKey(priority, _maxPriority), sequence));
            }
        }

        private static int NormalizePriority(int? priority, int maxPriority)
        {
            if (priority is null)
            {
                return 0;
            }

            return Math.Clamp(priority.Value, 0, maxPriority);
        }

        private static int CreatePriorityKey(int priority, int maxPriority) => maxPriority - priority;
    }
}
