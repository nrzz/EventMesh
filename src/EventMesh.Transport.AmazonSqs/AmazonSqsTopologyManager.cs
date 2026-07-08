using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AmazonSqs;

/// <summary>
/// Manages Amazon SQS queue topology and metadata used by the transport.
/// </summary>
public sealed class AmazonSqsTopologyManager
{
    private readonly IAmazonSQS _sqs;
    private readonly AmazonSqsTransportOptions _options;
    private readonly ILogger<AmazonSqsTopologyManager> _logger;
    private readonly ConcurrentDictionary<string, QueueTopology> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TopicTopology> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SubscriptionTopology> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _queueUrls = new(StringComparer.OrdinalIgnoreCase);

    public AmazonSqsTopologyManager(
        IAmazonSQS sqs,
        IOptions<AmazonSqsTransportOptions> options,
        ILogger<AmazonSqsTopologyManager> logger)
    {
        _sqs = sqs;
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
            _queueUrls.Clear();
        }

        var queueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var queue in topology.Queues)
        {
            var deadLetterDestination = queue.DeadLetterDestination
                ?? $"{queue.Name}{_options.DefaultDeadLetterSuffix}";

            _queues[queue.Name] = new QueueTopology
            {
                Name = queue.Name,
                Fifo = queue.Fifo,
                DeadLetterDestination = deadLetterDestination,
                MaxReceiveCount = _options.DefaultMaxReceiveCount,
            };

            queueNames.Add(queue.Name);
            queueNames.Add(deadLetterDestination);
        }

        foreach (var topic in topology.Topics)
        {
            _topics[topic.Name] = new TopicTopology
            {
                Name = topic.Name,
            };
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
            };

            queueNames.Add(destination);

            if (_topics.TryGetValue(subscription.Topic, out var topicTopology))
            {
                topicTopology.Subscriptions.Add(subscription.Name);
            }
        }

        foreach (var queueName in queueNames.OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            await EnsureQueueExistsAsync(queueName, cancellationToken).ConfigureAwait(false);
        }

        foreach (var queue in _queues.Values)
        {
            if (string.IsNullOrWhiteSpace(queue.DeadLetterDestination))
            {
                continue;
            }

            await ConfigureRedrivePolicyAsync(queue, cancellationToken).ConfigureAwait(false);
        }
    }

    public bool IsTopic(string destination) => _topics.ContainsKey(destination);

    public bool TryGetQueue(string name, out QueueTopology queue) => _queues.TryGetValue(name, out queue!);

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
            return new ReceiveTarget(subscription.Destination, subscription.Name);
        }

        return new ReceiveTarget(queueOrSubscription, queueOrSubscription);
    }

    public async Task<string> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken)
    {
        if (_queueUrls.TryGetValue(queueName, out var cachedUrl))
        {
            return cachedUrl;
        }

        try
        {
            var response = await _sqs.GetQueueUrlAsync(queueName, cancellationToken).ConfigureAwait(false);
            _queueUrls[queueName] = response.QueueUrl;
            return response.QueueUrl;
        }
        catch (QueueDoesNotExistException)
        {
            await EnsureQueueExistsAsync(queueName, cancellationToken).ConfigureAwait(false);
            var response = await _sqs.GetQueueUrlAsync(queueName, cancellationToken).ConfigureAwait(false);
            _queueUrls[queueName] = response.QueueUrl;
            return response.QueueUrl;
        }
    }

    public bool IsFifoQueue(string queueName) =>
        _queues.TryGetValue(queueName, out var queue) && queue.Fifo;

    private async Task EnsureQueueExistsAsync(string queueName, CancellationToken cancellationToken)
    {
        if (_queueUrls.ContainsKey(queueName))
        {
            return;
        }

        var fifo = _queues.TryGetValue(queueName, out var queueTopology) && queueTopology.Fifo;
        var attributes = new Dictionary<string, string>
        {
            ["VisibilityTimeout"] = _options.VisibilityTimeoutSeconds.ToString(CultureInfo.InvariantCulture),
        };

        if (fifo)
        {
            attributes["FifoQueue"] = "true";
            attributes["ContentBasedDeduplication"] = "true";
        }

        try
        {
            var response = await _sqs.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = attributes,
            }, cancellationToken).ConfigureAwait(false);

            _queueUrls[queueName] = response.QueueUrl;
        }
        catch (QueueNameExistsException)
        {
            var response = await _sqs.GetQueueUrlAsync(queueName, cancellationToken).ConfigureAwait(false);
            _queueUrls[queueName] = response.QueueUrl;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Failed to create Amazon SQS queue {QueueName}.", queueName);
            throw;
        }
    }

    private async Task ConfigureRedrivePolicyAsync(QueueTopology queue, CancellationToken cancellationToken)
    {
        var sourceUrl = await GetQueueUrlAsync(queue.Name, cancellationToken).ConfigureAwait(false);
        var deadLetterUrl = await GetQueueUrlAsync(queue.DeadLetterDestination, cancellationToken).ConfigureAwait(false);
        var deadLetterArn = await ResolveQueueArnAsync(queue.DeadLetterDestination, deadLetterUrl, cancellationToken)
            .ConfigureAwait(false);

        var redrivePolicy = JsonSerializer.Serialize(new
        {
            deadLetterTargetArn = deadLetterArn,
            maxReceiveCount = queue.MaxReceiveCount,
        });

        await _sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest
        {
            QueueUrl = sourceUrl,
            Attributes = new Dictionary<string, string>
            {
                ["RedrivePolicy"] = redrivePolicy,
            },
        }, cancellationToken).ConfigureAwait(false);
    }

    private async Task<string> ResolveQueueArnAsync(
        string queueName,
        string queueUrl,
        CancellationToken cancellationToken)
    {
        var attributes = await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = queueUrl,
            AttributeNames = ["QueueArn"],
        }, cancellationToken).ConfigureAwait(false);

        if (attributes.Attributes.TryGetValue("QueueArn", out var queueArn)
            && !string.IsNullOrWhiteSpace(queueArn))
        {
            return queueArn;
        }

        return $"arn:aws:sqs:{_options.Region}:{_options.AccountId}:{queueName}";
    }

    public sealed class QueueTopology
    {
        public required string Name { get; init; }

        public bool Fifo { get; init; }

        public required string DeadLetterDestination { get; init; }

        public int MaxReceiveCount { get; init; }
    }

    public sealed class TopicTopology
    {
        public required string Name { get; init; }

        public List<string> Subscriptions { get; } = [];
    }

    public sealed class SubscriptionTopology
    {
        public required string Name { get; init; }

        public required string Topic { get; init; }

        public required string Destination { get; init; }

        public string? Filter { get; init; }
    }

    public readonly record struct ReceiveTarget(string QueueName, string SubscriptionName);
}
