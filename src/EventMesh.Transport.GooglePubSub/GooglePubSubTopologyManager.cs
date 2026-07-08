using System.Collections.Concurrent;
using EventMesh.Abstractions.Transport;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.GooglePubSub;

/// <summary>
/// Manages Google Pub/Sub topic and subscription topology for the transport adapter.
/// </summary>
public sealed class GooglePubSubTopologyManager : IAsyncDisposable
{
    private readonly GooglePubSubTransportOptions _options;
    private readonly ILogger<GooglePubSubTopologyManager> _logger;
    private readonly ConcurrentDictionary<string, QueueTopology> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TopicTopology> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SubscriptionTopology> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _clientLock = new(1, 1);
    private PublisherServiceApiClient? _publisherApiClient;
    private SubscriberServiceApiClient? _subscriberApiClient;
    private int _disposed;

    public GooglePubSubTopologyManager(
        IOptions<GooglePubSubTransportOptions> options,
        ILogger<GooglePubSubTopologyManager> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public string ProjectId => _options.ProjectId;

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
        var subscriptionSpecs = new List<SubscriptionSpec>();

        foreach (var queue in topology.Queues)
        {
            var deadLetterDestination = queue.DeadLetterDestination
                ?? $"{queue.Name}{_options.DefaultDeadLetterSuffix}";

            _queues[queue.Name] = new QueueTopology
            {
                Name = queue.Name,
                TopicId = queue.Name,
                SubscriptionId = queue.Name,
                DeadLetterDestination = deadLetterDestination,
                MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
            };

            topicNames.Add(queue.Name);
            topicNames.Add(deadLetterDestination);
            subscriptionSpecs.Add(new SubscriptionSpec(queue.Name, queue.Name, Filter: null, DeadLetterDestination: deadLetterDestination));
            subscriptionSpecs.Add(new SubscriptionSpec(deadLetterDestination, deadLetterDestination, Filter: null, DeadLetterDestination: null));
        }

        foreach (var topic in topology.Topics)
        {
            _topics[topic.Name] = new TopicTopology
            {
                Name = topic.Name,
                TopicId = topic.Name,
            };

            topicNames.Add(topic.Name);
        }

        foreach (var subscription in topology.Subscriptions)
        {
            var destination = ResolveSubscriptionDestination(subscription);
            var subscriptionId = subscription.Name;

            _subscriptions[subscription.Name] = new SubscriptionTopology
            {
                Name = subscription.Name,
                TopicId = subscription.Topic,
                SubscriptionId = subscriptionId,
                Destination = destination,
                Filter = subscription.Filter,
            };

            topicNames.Add(subscription.Topic);
            subscriptionSpecs.Add(new SubscriptionSpec(
                subscriptionId,
                subscription.Topic,
                subscription.Filter,
                _queues.TryGetValue(destination, out var queueTopology) ? queueTopology.DeadLetterDestination : null));

            if (!_queues.ContainsKey(destination))
            {
                _queues[destination] = new QueueTopology
                {
                    Name = destination,
                    TopicId = destination,
                    SubscriptionId = destination,
                    DeadLetterDestination = $"{destination}{_options.DefaultDeadLetterSuffix}",
                    MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
                };

                topicNames.Add(destination);
                subscriptionSpecs.Add(new SubscriptionSpec(destination, destination, Filter: null, DeadLetterDestination: null));
            }

            if (_topics.TryGetValue(subscription.Topic, out var topicTopology))
            {
                topicTopology.Subscriptions.Add(subscription.Name);
            }
        }

        await EnsureTopicsExistAsync(topicNames, cancellationToken);
        await EnsureSubscriptionsExistAsync(subscriptionSpecs, cancellationToken);
    }

    public bool TryGetQueue(string name, out QueueTopology queue) => _queues.TryGetValue(name, out queue!);

    public bool TryGetTopic(string name, out TopicTopology topic) => _topics.TryGetValue(name, out topic!);

    public bool TryGetSubscription(string name, out SubscriptionTopology subscription) =>
        _subscriptions.TryGetValue(name, out subscription!);

    public bool IsTopic(string destination) => _topics.ContainsKey(destination);

    public ReceiveTarget ResolveReceiveTarget(string queueOrSubscription)
    {
        if (_subscriptions.TryGetValue(queueOrSubscription, out var subscription))
        {
            return new ReceiveTarget(
                subscription.SubscriptionId,
                subscription.TopicId,
                subscription.Destination);
        }

        if (_queues.TryGetValue(queueOrSubscription, out var queue))
        {
            return new ReceiveTarget(queue.SubscriptionId, queue.TopicId, queue.Name);
        }

        return new ReceiveTarget(queueOrSubscription, queueOrSubscription, queueOrSubscription);
    }

    public string ResolveReplaySubscriptionId(string source)
    {
        if (_queues.TryGetValue(source, out var queue))
        {
            return queue.SubscriptionId;
        }

        if (_subscriptions.TryGetValue(source, out var subscription))
        {
            return subscription.SubscriptionId;
        }

        return source;
    }

    public TopicName GetTopicName(string topicId) => TopicName.FromProjectTopic(_options.ProjectId, topicId);

    public SubscriptionName GetSubscriptionName(string subscriptionId) =>
        SubscriptionName.FromProjectSubscription(_options.ProjectId, subscriptionId);

    public async Task<PublisherServiceApiClient> GetPublisherApiClientAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (_publisherApiClient is not null)
        {
            return _publisherApiClient;
        }

        await _clientLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_publisherApiClient is not null)
            {
                return _publisherApiClient;
            }

            _publisherApiClient = await new PublisherServiceApiClientBuilder
            {
                EmulatorDetection = _options.EmulatorDetection,
            }.BuildAsync(cancellationToken).ConfigureAwait(false);

            return _publisherApiClient;
        }
        finally
        {
            _clientLock.Release();
        }
    }

    public async Task<SubscriberServiceApiClient> GetSubscriberApiClientAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (_subscriberApiClient is not null)
        {
            return _subscriberApiClient;
        }

        await _clientLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_subscriberApiClient is not null)
            {
                return _subscriberApiClient;
            }

            _subscriberApiClient = await new SubscriberServiceApiClientBuilder
            {
                EmulatorDetection = _options.EmulatorDetection,
            }.BuildAsync(cancellationToken).ConfigureAwait(false);

            return _subscriberApiClient;
        }
        finally
        {
            _clientLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _publisherApiClient = null;
        _subscriberApiClient = null;
        _clientLock.Dispose();
        await Task.CompletedTask;
    }

    private async Task EnsureTopicsExistAsync(IEnumerable<string> topicIds, CancellationToken cancellationToken)
    {
        var publisherApi = await GetPublisherApiClientAsync(cancellationToken).ConfigureAwait(false);

        foreach (var topicId in topicIds.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var topicName = GetTopicName(topicId);

            try
            {
                await publisherApi.CreateTopicAsync(topicName, cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode == StatusCode.AlreadyExists)
            {
                _logger.LogDebug("Pub/Sub topic {Topic} already exists.", topicName);
            }
        }
    }

    private async Task EnsureSubscriptionsExistAsync(
        IEnumerable<SubscriptionSpec> subscriptionSpecs,
        CancellationToken cancellationToken)
    {
        var subscriberApi = await GetSubscriberApiClientAsync(cancellationToken).ConfigureAwait(false);

        foreach (var spec in subscriptionSpecs
                     .GroupBy(spec => spec.SubscriptionId, StringComparer.OrdinalIgnoreCase)
                     .Select(group => group.First()))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var subscriptionName = GetSubscriptionName(spec.SubscriptionId);
            var topicName = GetTopicName(spec.TopicId);
            var filter = GooglePubSubMessageCodec.BuildSubscriptionFilter(spec.Filter);

            var subscription = new Subscription
            {
                SubscriptionName = subscriptionName,
                TopicAsTopicName = topicName,
                AckDeadlineSeconds = _options.AckDeadlineSeconds,
                EnableMessageOrdering = _options.EnableMessageOrdering,
            };

            if (!string.IsNullOrWhiteSpace(filter))
            {
                subscription.Filter = filter;
            }

            if (!string.IsNullOrWhiteSpace(spec.DeadLetterDestination))
            {
                subscription.DeadLetterPolicy = new DeadLetterPolicy
                {
                    DeadLetterTopic = GetTopicName(spec.DeadLetterDestination).ToString(),
                    MaxDeliveryAttempts = _options.DefaultMaxDeliveryAttempts,
                };
            }

            try
            {
                await subscriberApi.CreateSubscriptionAsync(subscription, cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode == StatusCode.AlreadyExists)
            {
                _logger.LogDebug("Pub/Sub subscription {Subscription} already exists.", subscriptionName);
            }
        }
    }

    private static string ResolveSubscriptionDestination(SubscriptionDefinition subscription) =>
        subscription.Destination ?? subscription.Name;

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(GooglePubSubTopologyManager));
        }
    }

    private readonly record struct SubscriptionSpec(
        string SubscriptionId,
        string TopicId,
        string? Filter,
        string? DeadLetterDestination);

    public sealed class QueueTopology
    {
        public required string Name { get; init; }

        public required string TopicId { get; init; }

        public required string SubscriptionId { get; init; }

        public required string DeadLetterDestination { get; init; }

        public int MaxDeliveryAttempts { get; init; }
    }

    public sealed class TopicTopology
    {
        public required string Name { get; init; }

        public required string TopicId { get; init; }

        public List<string> Subscriptions { get; } = [];
    }

    public sealed class SubscriptionTopology
    {
        public required string Name { get; init; }

        public required string TopicId { get; init; }

        public required string SubscriptionId { get; init; }

        public required string Destination { get; init; }

        public string? Filter { get; init; }
    }

    public readonly record struct ReceiveTarget(
        string SubscriptionId,
        string TopicId,
        string LogicalDestination);
}
