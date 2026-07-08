using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.Kafka;

/// <summary>
/// Manages Kafka topic topology and metadata used by the transport.
/// </summary>
public sealed class KafkaTopologyManager : IDisposable
{
    private readonly KafkaTransportOptions _options;
    private readonly ILogger<KafkaTopologyManager> _logger;
    private readonly ConcurrentDictionary<string, QueueTopology> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TopicTopology> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SubscriptionTopology> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _adminLock = new();
    private IAdminClient? _adminClient;
    private int _disposed;

    public KafkaTopologyManager(IOptions<KafkaTransportOptions> options, ILogger<KafkaTopologyManager> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topology);
        cancellationToken.ThrowIfCancellationRequested();

        if (topology.ReplaceExisting)
        {
            _queues.Clear();
            _topics.Clear();
            _subscriptions.Clear();
        }

        var topicNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var queue in topology.Queues)
        {
            var deadLetterDestination = queue.DeadLetterDestination
                ?? $"{queue.Name}{_options.DefaultDeadLetterSuffix}";

            _queues[queue.Name] = new QueueTopology
            {
                Name = queue.Name,
                DeadLetterDestination = deadLetterDestination,
                MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
            };

            topicNames.Add(queue.Name);
            topicNames.Add(deadLetterDestination);
        }

        foreach (var topic in topology.Topics)
        {
            _topics[topic.Name] = new TopicTopology
            {
                Name = topic.Name,
                PartitionCount = topic.PartitionCount ?? _options.DefaultPartitionCount,
            };

            topicNames.Add(topic.Name);
        }

        foreach (var subscription in topology.Subscriptions)
        {
            var destination = ResolveSubscriptionDestination(subscription);
            _subscriptions[subscription.Name] = new SubscriptionTopology
            {
                Name = subscription.Name,
                Topic = subscription.Topic,
                Destination = destination,
                Filter = subscription.Filter,
                ConsumerGroup = subscription.ConsumerGroup ?? subscription.Name,
            };

            topicNames.Add(subscription.Topic);
            topicNames.Add(destination);

            if (_topics.TryGetValue(subscription.Topic, out var topicTopology))
            {
                topicTopology.Subscriptions.Add(subscription.Name);
            }
        }

        await EnsureTopicsExistAsync(topicNames, topology, cancellationToken);
    }

    public bool TryGetQueue(string name, out QueueTopology queue) => _queues.TryGetValue(name, out queue!);

    public bool TryGetTopic(string name, out TopicTopology topic) => _topics.TryGetValue(name, out topic!);

    public bool TryGetSubscription(string name, out SubscriptionTopology subscription) =>
        _subscriptions.TryGetValue(name, out subscription!);

    public IEnumerable<SubscriptionTopology> GetSubscriptionsForTopic(string topicName)
    {
        if (!_topics.TryGetValue(topicName, out var topic))
        {
            yield break;
        }

        foreach (var subscriptionName in topic.Subscriptions)
        {
            if (_subscriptions.TryGetValue(subscriptionName, out var subscription))
            {
                yield return subscription;
            }
        }
    }

    public ReceiveTarget ResolveReceiveTarget(string queueOrSubscription)
    {
        if (_subscriptions.TryGetValue(queueOrSubscription, out var subscription))
        {
            return new ReceiveTarget(subscription.Destination, subscription.ConsumerGroup);
        }

        var consumerGroup = $"{_options.GroupId}-{queueOrSubscription}";
        return new ReceiveTarget(queueOrSubscription, consumerGroup);
    }

    public string ResolveConsumerGroup(string queueOrSubscription) =>
        ResolveReceiveTarget(queueOrSubscription).ConsumerGroup;

    public string ResolveTopic(string queueOrSubscription) =>
        ResolveReceiveTarget(queueOrSubscription).Topic;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        lock (_adminLock)
        {
            _adminClient?.Dispose();
            _adminClient = null;
        }
    }

    private async Task EnsureTopicsExistAsync(
        IEnumerable<string> topicNames,
        TopologyDefinition topology,
        CancellationToken cancellationToken)
    {
        var specifications = topicNames
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(topicName => new TopicSpecification
            {
                Name = topicName,
                NumPartitions = ResolvePartitionCount(topicName, topology),
                ReplicationFactor = _options.ReplicationFactor,
            })
            .ToList();

        if (specifications.Count == 0)
        {
            return;
        }

        var admin = GetAdminClient();
        try
        {
            await admin.CreateTopicsAsync(specifications).ConfigureAwait(false);
        }
        catch (CreateTopicsException exception)
        {
            foreach (var result in exception.Results)
            {
                if (result.Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    continue;
                }

                if (result.Error.IsError)
                {
                    _logger.LogWarning(
                        "Failed to create Kafka topic {Topic}: {Reason}",
                        result.Topic,
                        result.Error.Reason);
                }
            }
        }
    }

    private int ResolvePartitionCount(string topicName, TopologyDefinition topology)
    {
        var topicDefinition = topology.Topics.FirstOrDefault(topic =>
            string.Equals(topic.Name, topicName, StringComparison.OrdinalIgnoreCase));

        if (topicDefinition?.PartitionCount is not null)
        {
            return topicDefinition.PartitionCount.Value;
        }

        if (_topics.TryGetValue(topicName, out var topicTopology))
        {
            return topicTopology.PartitionCount;
        }

        return _options.DefaultPartitionCount;
    }

    private static string ResolveSubscriptionDestination(SubscriptionDefinition subscription) =>
        subscription.Destination ?? subscription.Name;

    private IAdminClient GetAdminClient()
    {
        lock (_adminLock)
        {
            if (_adminClient is not null)
            {
                return _adminClient;
            }

            _adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _options.BootstrapServers,
            }).Build();

            return _adminClient;
        }
    }

    public sealed class QueueTopology
    {
        public required string Name { get; init; }

        public required string DeadLetterDestination { get; init; }

        public int MaxDeliveryAttempts { get; init; }
    }

    public sealed class TopicTopology
    {
        public required string Name { get; init; }

        public int PartitionCount { get; init; }

        public List<string> Subscriptions { get; } = [];
    }

    public sealed class SubscriptionTopology
    {
        public required string Name { get; init; }

        public required string Topic { get; init; }

        public required string Destination { get; init; }

        public string? Filter { get; init; }

        public required string ConsumerGroup { get; init; }
    }

    public readonly record struct ReceiveTarget(string Topic, string ConsumerGroup);
}
