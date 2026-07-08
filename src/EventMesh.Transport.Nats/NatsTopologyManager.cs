using System.Collections.Concurrent;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;

namespace EventMesh.Transport.Nats;

/// <summary>
/// Manages JetStream stream and consumer topology for the NATS transport.
/// </summary>
public sealed class NatsTopologyManager
{
    private readonly NatsConnectionManager _connectionManager;
    private readonly NatsTransportOptions _options;
    private readonly ConcurrentDictionary<string, QueueTopology> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TopicTopology> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SubscriptionTopology> _subscriptions = new(StringComparer.OrdinalIgnoreCase);

    public NatsTopologyManager(
        NatsConnectionManager connectionManager,
        IOptions<NatsTransportOptions> options)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
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

        var streamConfigs = new Dictionary<string, StreamConfig>(StringComparer.OrdinalIgnoreCase);

        foreach (var queue in topology.Queues)
        {
            var deadLetterDestination = queue.DeadLetterDestination
                ?? $"{queue.Name}{_options.DefaultDeadLetterSuffix}";

            _queues[queue.Name] = new QueueTopology
            {
                Name = queue.Name,
                Subject = ToSubject(queue.Name),
                StreamName = ToStreamName(queue.Name),
                DeadLetterDestination = deadLetterDestination,
                MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
            };

            AddStreamConfig(streamConfigs, queue.Name);
            AddStreamConfig(streamConfigs, deadLetterDestination);
        }

        foreach (var topic in topology.Topics)
        {
            _topics[topic.Name] = new TopicTopology
            {
                Name = topic.Name,
                Subject = ToSubject(topic.Name),
                StreamName = ToStreamName(topic.Name),
            };

            AddStreamConfig(streamConfigs, topic.Name);
        }

        foreach (var subscription in topology.Subscriptions)
        {
            var destination = subscription.Destination ?? subscription.Name;
            _subscriptions[subscription.Name] = new SubscriptionTopology
            {
                Name = subscription.Name,
                Topic = subscription.Topic,
                Destination = destination,
                Filter = subscription.Filter,
                ConsumerGroup = subscription.ConsumerGroup ?? subscription.Name,
            };

            AddStreamConfig(streamConfigs, subscription.Topic);
            AddStreamConfig(streamConfigs, destination);

            if (_topics.TryGetValue(subscription.Topic, out var topicTopology))
            {
                topicTopology.Subscriptions.Add(subscription.Name);
            }
        }

        var client = await _connectionManager.GetClientAsync(cancellationToken).ConfigureAwait(false);
        var js = client.CreateJetStreamContext();

        foreach (var streamConfig in streamConfigs.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await js.CreateOrUpdateStreamAsync(streamConfig, cancellationToken).ConfigureAwait(false);
        }

        foreach (var queue in _queues.Values)
        {
            await EnsureConsumerAsync(js, queue.StreamName, queue.Subject, ResolveConsumerName(queue.Name), queue.MaxDeliveryAttempts, cancellationToken)
                .ConfigureAwait(false);
            await EnsureConsumerAsync(
                    js,
                    ToStreamName(queue.DeadLetterDestination),
                    ToSubject(queue.DeadLetterDestination),
                    ResolveConsumerName(queue.DeadLetterDestination),
                    queue.MaxDeliveryAttempts,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (var subscription in _subscriptions.Values)
        {
            var destination = subscription.Destination;
            var queueState = _queues.GetValueOrDefault(destination);
            var maxDeliver = queueState?.MaxDeliveryAttempts ?? _options.DefaultMaxDeliveryAttempts;
            await EnsureConsumerAsync(
                    js,
                    ToStreamName(destination),
                    ToSubject(destination),
                    subscription.ConsumerGroup,
                    maxDeliver,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public bool TryGetQueue(string name, out QueueTopology queue) => _queues.TryGetValue(name, out queue!);

    public bool TryGetTopic(string name, out TopicTopology topic) => _topics.TryGetValue(name, out topic!);

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
            return new ReceiveTarget(
                ToStreamName(subscription.Destination),
                ToSubject(subscription.Destination),
                subscription.ConsumerGroup,
                subscription.Destination);
        }

        var consumerName = ResolveConsumerName(queueOrSubscription);
        return new ReceiveTarget(
            ToStreamName(queueOrSubscription),
            ToSubject(queueOrSubscription),
            consumerName,
            queueOrSubscription);
    }

    public string ToStreamName(string logicalName)
    {
        var sanitized = logicalName
            .Replace(".", "_", StringComparison.Ordinal)
            .Replace("*", "_", StringComparison.Ordinal)
            .Replace(">", "_", StringComparison.Ordinal)
            .Replace("/", "_", StringComparison.Ordinal)
            .Replace("\\", "_", StringComparison.Ordinal);

        return $"{_options.StreamPrefix}{sanitized}";
    }

    public string ToSubject(string logicalName) => logicalName;

    public string ResolveConsumerName(string queueOrSubscription) =>
        SanitizeConsumerName($"{_options.ConsumerPrefix}-{queueOrSubscription}");

    private static string SanitizeConsumerName(string name) =>
        name
            .Replace(".", "-", StringComparison.Ordinal)
            .Replace("*", "-", StringComparison.Ordinal)
            .Replace(">", "-", StringComparison.Ordinal)
            .Replace("/", "-", StringComparison.Ordinal)
            .Replace("\\", "-", StringComparison.Ordinal);

    private void AddStreamConfig(IDictionary<string, StreamConfig> streamConfigs, string logicalName)
    {
        var streamName = ToStreamName(logicalName);
        if (streamConfigs.ContainsKey(streamName))
        {
            return;
        }

        streamConfigs[streamName] = new StreamConfig(name: streamName, subjects: [ToSubject(logicalName)])
        {
            Storage = StreamConfigStorage.File,
            Retention = StreamConfigRetention.Limits,
        };
    }

    private static async Task EnsureConsumerAsync(
        INatsJSContext js,
        string streamName,
        string subject,
        string durableName,
        int maxDeliver,
        CancellationToken cancellationToken)
    {
        await js.CreateOrUpdateConsumerAsync(
            streamName,
            new ConsumerConfig(durableName)
            {
                FilterSubject = subject,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                MaxDeliver = maxDeliver,
                AckWait = TimeSpan.FromSeconds(30),
            },
            cancellationToken).ConfigureAwait(false);
    }

    public sealed class QueueTopology
    {
        public required string Name { get; init; }

        public required string Subject { get; init; }

        public required string StreamName { get; init; }

        public required string DeadLetterDestination { get; init; }

        public int MaxDeliveryAttempts { get; init; }
    }

    public sealed class TopicTopology
    {
        public required string Name { get; init; }

        public required string Subject { get; init; }

        public required string StreamName { get; init; }

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

    public readonly record struct ReceiveTarget(string StreamName, string Subject, string ConsumerName, string QueueName);
}
