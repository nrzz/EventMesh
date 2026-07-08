using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventMesh.Transport.RedisStreams;

/// <summary>
/// Provisions and tracks Redis Streams topology for the transport adapter.
/// </summary>
public sealed class RedisStreamsTopologyManager
{
    private readonly IConnectionMultiplexer _connection;
    private readonly RedisStreamsTransportOptions _options;
    private readonly ILogger<RedisStreamsTopologyManager> _logger;
    private readonly Dictionary<string, QueueState> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TopicState> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SubscriptionState> _subscriptions = new(StringComparer.OrdinalIgnoreCase);

    public RedisStreamsTopologyManager(
        IConnectionMultiplexer connection,
        IOptions<RedisStreamsTransportOptions> options,
        ILogger<RedisStreamsTopologyManager> logger)
    {
        _connection = connection;
        _options = options.Value;
        _logger = logger;
    }

    public async Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topology);
        cancellationToken.ThrowIfCancellationRequested();

        if (topology.ReplaceExisting)
        {
            await ClearTopologyAsync(cancellationToken);
            _queues.Clear();
            _topics.Clear();
            _subscriptions.Clear();
        }

        foreach (var queue in topology.Queues)
        {
            var streamKey = ToStreamKey(queue.Name);
            var deadLetterDestination = queue.DeadLetterDestination
                ?? $"{queue.Name}{_options.DefaultDeadLetterSuffix}";

            _queues[queue.Name] = new QueueState
            {
                Name = queue.Name,
                StreamKey = streamKey,
                ConsumerGroup = ToConsumerGroup(queue.Name),
                DeadLetterDestination = deadLetterDestination,
                MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
            };

            await EnsureConsumerGroupAsync(streamKey, ToConsumerGroup(queue.Name), cancellationToken);
            await EnsureConsumerGroupAsync(ToStreamKey(deadLetterDestination), ToConsumerGroup(deadLetterDestination), cancellationToken);
        }

        foreach (var topic in topology.Topics)
        {
            var streamKey = ToStreamKey(topic.Name);
            _topics[topic.Name] = new TopicState
            {
                Name = topic.Name,
                StreamKey = streamKey,
            };

            await EnsureStreamAsync(streamKey, cancellationToken);

            if (!_queues.ContainsKey(topic.Name))
            {
                _queues[topic.Name] = new QueueState
                {
                    Name = topic.Name,
                    StreamKey = streamKey,
                    ConsumerGroup = ToConsumerGroup(topic.Name),
                    DeadLetterDestination = $"{topic.Name}{_options.DefaultDeadLetterSuffix}",
                    MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
                };
                await EnsureConsumerGroupAsync(streamKey, ToConsumerGroup(topic.Name), cancellationToken);
            }
        }

        foreach (var subscription in topology.Subscriptions)
        {
            var destination = ResolveSubscriptionDestination(subscription);
            var destinationStream = ToStreamKey(destination);
            var consumerGroup = ToConsumerGroup(subscription.Name, subscription.ConsumerGroup);

            _subscriptions[subscription.Name] = new SubscriptionState
            {
                Name = subscription.Name,
                Topic = subscription.Topic,
                Destination = destination,
                DestinationStreamKey = destinationStream,
                ConsumerGroup = consumerGroup,
                Filter = subscription.Filter,
            };

            if (!_queues.ContainsKey(destination))
            {
                _queues[destination] = new QueueState
                {
                    Name = destination,
                    StreamKey = destinationStream,
                    ConsumerGroup = ToConsumerGroup(destination),
                    DeadLetterDestination = $"{destination}{_options.DefaultDeadLetterSuffix}",
                    MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
                };
            }

            await EnsureConsumerGroupAsync(destinationStream, consumerGroup, cancellationToken);

            if (_topics.TryGetValue(subscription.Topic, out var topic))
            {
                topic.Subscriptions.Add(subscription.Name);
            }
        }
    }

    public bool IsTopic(string destination) => _topics.ContainsKey(destination);

    public IReadOnlyList<SubscriptionState> GetMatchingSubscriptions(string topicName, string? routingKey)
    {
        if (!_topics.TryGetValue(topicName, out var topic))
        {
            return Array.Empty<SubscriptionState>();
        }

        var matches = new List<SubscriptionState>();
        foreach (var subscriptionName in topic.Subscriptions)
        {
            if (!_subscriptions.TryGetValue(subscriptionName, out var subscription))
            {
                continue;
            }

            if (MatchesFilter(routingKey, subscription.Filter))
            {
                matches.Add(subscription);
            }
        }

        return matches;
    }

    public ReceiveTarget ResolveReceiveTarget(string queueOrSubscription)
    {
        if (_subscriptions.TryGetValue(queueOrSubscription, out var subscription))
        {
            return new ReceiveTarget(
                subscription.Destination,
                subscription.DestinationStreamKey,
                subscription.ConsumerGroup);
        }

        if (_queues.TryGetValue(queueOrSubscription, out var queue))
        {
            return new ReceiveTarget(queue.Name, queue.StreamKey, queue.ConsumerGroup);
        }

        var streamKey = ToStreamKey(queueOrSubscription);
        var consumerGroup = ToConsumerGroup(queueOrSubscription);
        return new ReceiveTarget(queueOrSubscription, streamKey, consumerGroup);
    }

    public QueueState GetQueueState(string queueName)
    {
        if (_queues.TryGetValue(queueName, out var queue))
        {
            return queue;
        }

        var streamKey = ToStreamKey(queueName);
        return new QueueState
        {
            Name = queueName,
            StreamKey = streamKey,
            ConsumerGroup = ToConsumerGroup(queueName),
            DeadLetterDestination = $"{queueName}{_options.DefaultDeadLetterSuffix}",
            MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
        };
    }

    public RedisKey ToStreamKey(string name) => $"{_options.StreamPrefix}{name}";

    public RedisKey ToPriorityStreamKey(string name, int priority) =>
        $"{_options.StreamPrefix}{name}:p{priority}";

    public string ToConsumerGroup(string name, string? consumerGroup = null) =>
        string.IsNullOrWhiteSpace(consumerGroup) ? $"{name}-group" : consumerGroup;

    private static string ResolveSubscriptionDestination(SubscriptionDefinition subscription) =>
        subscription.Destination ?? subscription.Name;

    private static bool MatchesFilter(string? routingKey, string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter) || filter is "#" or "*")
        {
            return true;
        }

        return string.Equals(routingKey, filter, StringComparison.OrdinalIgnoreCase);
    }

    private async Task EnsureStreamAsync(RedisKey streamKey, CancellationToken cancellationToken)
    {
        var database = _connection.GetDatabase();
        await database.StreamAddAsync(streamKey, "__bootstrap", "1").WaitAsync(cancellationToken);
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

    private async Task ClearTopologyAsync(CancellationToken cancellationToken)
    {
        var database = _connection.GetDatabase();
        var endpoints = _connection.GetEndPoints();
        if (endpoints.Length == 0)
        {
            return;
        }

        var server = _connection.GetServer(endpoints[0]);
        await foreach (var key in server.KeysAsync(pattern: $"{_options.StreamPrefix}*"))
        {
            cancellationToken.ThrowIfCancellationRequested();
            await database.KeyDeleteAsync(key).WaitAsync(cancellationToken);
        }

        await database.KeyDeleteAsync(_options.DelayedMessagesKey).WaitAsync(cancellationToken);
    }

    public sealed class QueueState
    {
        public required string Name { get; init; }

        public required RedisKey StreamKey { get; init; }

        public required string ConsumerGroup { get; init; }

        public required string DeadLetterDestination { get; init; }

        public int MaxDeliveryAttempts { get; init; }
    }

    public sealed class TopicState
    {
        public required string Name { get; init; }

        public required RedisKey StreamKey { get; init; }

        public List<string> Subscriptions { get; } = [];
    }

    public sealed class SubscriptionState
    {
        public required string Name { get; init; }

        public required string Topic { get; init; }

        public required string Destination { get; init; }

        public required RedisKey DestinationStreamKey { get; init; }

        public required string ConsumerGroup { get; init; }

        public string? Filter { get; init; }
    }

    public readonly record struct ReceiveTarget(
        string QueueName,
        RedisKey StreamKey,
        string ConsumerGroup);
}
