using Azure.Messaging.ServiceBus.Administration;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AzureServiceBus;

/// <summary>
/// Provisions Azure Service Bus queues, topics, and subscriptions.
/// </summary>
public sealed class AzureServiceBusTopologyManager
{
    private const string DefaultRuleName = "$Default";
    private const string RoutingKeyPropertyName = "routingKey";

    private readonly AzureServiceBusConnection _connection;
    private readonly AzureServiceBusTransportOptions _options;
    private readonly ILogger<AzureServiceBusTopologyManager> _logger;
    private readonly SemaphoreSlim _topologyLock = new(1, 1);

    public AzureServiceBusTopologyManager(
        AzureServiceBusConnection connection,
        IOptions<AzureServiceBusTransportOptions> options,
        ILogger<AzureServiceBusTopologyManager> logger)
    {
        _connection = connection;
        _options = options.Value;
        _logger = logger;
    }

    public AzureServiceBusTopologyState State { get; } = new();

    public async Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topology);
        cancellationToken.ThrowIfCancellationRequested();

        await _topologyLock.WaitAsync(cancellationToken);
        try
        {
            var administrationClient = _connection.AdministrationClient;

            if (topology.ReplaceExisting)
            {
                await DeleteTopologyAsync(administrationClient, topology, cancellationToken);
                State.Clear();
            }

            foreach (var queue in topology.Queues)
            {
                var deadLetterDestination = queue.DeadLetterDestination
                    ?? $"{queue.Name}{_options.DefaultDeadLetterSuffix}";

                var queueOptions = new CreateQueueOptions(queue.Name)
                {
                    DeadLetteringOnMessageExpiration = true,
                    EnableBatchedOperations = true,
                    RequiresSession = queue.Fifo || queue.Arguments.ContainsKey("requiresSession"),
                };

                if (queue.DefaultTimeToLive is not null)
                {
                    queueOptions.DefaultMessageTimeToLive = queue.DefaultTimeToLive.Value;
                }

                if (queue.MaxLength is not null)
                {
                    queueOptions.MaxSizeInMegabytes = ConvertMaxLengthToMegabytes(queue.MaxLength.Value);
                }

                await administrationClient.CreateQueueAsync(queueOptions, cancellationToken);
                State.RegisterQueue(queue.Name, deadLetterDestination, queueOptions.RequiresSession);
            }

            foreach (var topic in topology.Topics)
            {
                var topicOptions = new CreateTopicOptions(topic.Name)
                {
                    EnableBatchedOperations = true,
                };

                await administrationClient.CreateTopicAsync(topicOptions, cancellationToken);
                State.RegisterTopic(topic.Name);
            }

            foreach (var subscription in topology.Subscriptions)
            {
                var subscriptionName = subscription.Name;
                var subscriptionOptions = new CreateSubscriptionOptions(subscription.Topic, subscriptionName)
                {
                    DeadLetteringOnMessageExpiration = true,
                    EnableBatchedOperations = true,
                };

                await administrationClient.CreateSubscriptionAsync(subscriptionOptions, cancellationToken);

                var ruleFilter = BuildRuleFilter(subscription.Filter);
                await administrationClient.DeleteRuleAsync(
                    subscription.Topic,
                    subscriptionName,
                    DefaultRuleName,
                    cancellationToken);

                await administrationClient.CreateRuleAsync(
                    subscription.Topic,
                    subscriptionName,
                    new CreateRuleOptions(DefaultRuleName)
                    {
                        Filter = ruleFilter,
                    },
                    cancellationToken);

                var destination = subscription.Destination ?? subscription.Name;
                State.RegisterSubscription(subscription.Name, subscription.Topic, subscriptionName, destination);
            }
        }
        finally
        {
            _topologyLock.Release();
        }
    }

    public ReceiveTarget ResolveReceiveTarget(string queueOrSubscription)
    {
        if (State.TryResolveDeadLetterSource(queueOrSubscription, out var deadLetterSource))
        {
            return ReceiveTarget.ForDeadLetterQueue(deadLetterSource, queueOrSubscription);
        }

        if (State.TryGetSubscription(queueOrSubscription, out var subscription))
        {
            return ReceiveTarget.ForSubscription(subscription.Topic, subscription.SubscriptionName, queueOrSubscription);
        }

        var requiresSession = State.RequiresSession(queueOrSubscription);
        return ReceiveTarget.ForQueue(queueOrSubscription, requiresSession);
    }

    public SendTarget ResolveSendTarget(string destination)
    {
        if (State.IsTopic(destination))
        {
            return SendTarget.ForTopic(destination);
        }

        return SendTarget.ForQueue(destination);
    }

    public string? GetDeadLetterDestination(string queueName) => State.GetDeadLetterDestination(queueName);

    private static RuleFilter BuildRuleFilter(string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter) || string.Equals(filter, "#", StringComparison.Ordinal))
        {
            return new TrueRuleFilter();
        }

        if (filter.Contains('=') || filter.Contains("AND", StringComparison.OrdinalIgnoreCase))
        {
            return new SqlRuleFilter(filter);
        }

        return new SqlRuleFilter($"sys.Label = '{EscapeSqlLiteral(filter)}'");
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private static int ConvertMaxLengthToMegabytes(long maxLength)
    {
        const long bytesPerMegabyte = 1024 * 1024;
        var estimatedBytes = maxLength * 1024;
        return (int)Math.Max(1, Math.Ceiling(estimatedBytes / (double)bytesPerMegabyte));
    }

    private async Task DeleteTopologyAsync(
        ServiceBusAdministrationClient administrationClient,
        TopologyDefinition topology,
        CancellationToken cancellationToken)
    {
        foreach (var subscription in topology.Subscriptions)
        {
            try
            {
                if (await administrationClient.SubscriptionExistsAsync(subscription.Topic, subscription.Name, cancellationToken))
                {
                    await administrationClient.DeleteSubscriptionAsync(subscription.Topic, subscription.Name, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to delete subscription {Topic}/{Subscription}", subscription.Topic, subscription.Name);
            }
        }

        foreach (var topic in topology.Topics)
        {
            try
            {
                if (await administrationClient.TopicExistsAsync(topic.Name, cancellationToken))
                {
                    await administrationClient.DeleteTopicAsync(topic.Name, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to delete topic {Topic}", topic.Name);
            }
        }

        foreach (var queue in topology.Queues)
        {
            try
            {
                if (await administrationClient.QueueExistsAsync(queue.Name, cancellationToken))
                {
                    await administrationClient.DeleteQueueAsync(queue.Name, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to delete queue {Queue}", queue.Name);
            }
        }
    }
}

/// <summary>
/// Tracks provisioned Azure Service Bus topology metadata.
/// </summary>
public sealed class AzureServiceBusTopologyState
{
    private readonly HashSet<string> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, QueueTopology> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SubscriptionTopology> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string> _deadLetterAliases = new(StringComparer.OrdinalIgnoreCase);

    public void Clear()
    {
        _topics.Clear();
        _queues.Clear();
        _subscriptions.Clear();
        _deadLetterAliases.Clear();
    }

    public void RegisterTopic(string name) => _topics.Add(name);

    public void RegisterQueue(string name, string deadLetterDestination, bool requiresSession)
    {
        _queues[name] = new QueueTopology
        {
            Name = name,
            DeadLetterDestination = deadLetterDestination,
            RequiresSession = requiresSession,
        };

        _deadLetterAliases[deadLetterDestination] = name;
    }

    public void RegisterSubscription(string name, string topic, string subscriptionName, string destination) =>
        _subscriptions[name] = new SubscriptionTopology
        {
            Name = name,
            Topic = topic,
            SubscriptionName = subscriptionName,
            Destination = destination,
        };

    public bool IsTopic(string destination) => _topics.Contains(destination);

    public bool RequiresSession(string queueName) =>
        _queues.TryGetValue(queueName, out var queue) && queue.RequiresSession;

    public string? GetDeadLetterDestination(string queueName) =>
        _queues.TryGetValue(queueName, out var queue) ? queue.DeadLetterDestination : null;

    public bool TryGetSubscription(string name, out SubscriptionTopology subscription) =>
        _subscriptions.TryGetValue(name, out subscription!);

    public bool TryResolveDeadLetterSource(string queueOrSubscription, out string sourceQueue) =>
        _deadLetterAliases.TryGetValue(queueOrSubscription, out sourceQueue!);

    public sealed class QueueTopology
    {
        public required string Name { get; init; }
        public required string DeadLetterDestination { get; init; }
        public bool RequiresSession { get; init; }
    }

    public sealed class SubscriptionTopology
    {
        public required string Name { get; init; }
        public required string Topic { get; init; }
        public required string SubscriptionName { get; init; }
        public required string Destination { get; init; }
    }
}

/// <summary>
/// Describes where messages are received from in Azure Service Bus.
/// </summary>
public readonly record struct ReceiveTarget
{
    public ReceiveTargetKind Kind { get; init; }
    public string EntityPath { get; init; }
    public string LogicalName { get; init; }
    public bool RequiresSession { get; init; }
    public string? DeadLetterSourceQueue { get; init; }

    public static ReceiveTarget ForQueue(string queueName, bool requiresSession) => new()
    {
        Kind = ReceiveTargetKind.Queue,
        EntityPath = queueName,
        LogicalName = queueName,
        RequiresSession = requiresSession,
    };

    public static ReceiveTarget ForSubscription(string topic, string subscriptionName, string logicalName) => new()
    {
        Kind = ReceiveTargetKind.Subscription,
        EntityPath = $"{topic}/Subscriptions/{subscriptionName}",
        LogicalName = logicalName,
        RequiresSession = false,
    };

    public static ReceiveTarget ForDeadLetterQueue(string sourceQueue, string logicalName) => new()
    {
        Kind = ReceiveTargetKind.DeadLetter,
        EntityPath = sourceQueue,
        LogicalName = logicalName,
        DeadLetterSourceQueue = sourceQueue,
        RequiresSession = false,
    };
}

public enum ReceiveTargetKind
{
    Queue,
    Subscription,
    DeadLetter,
}

/// <summary>
/// Describes where messages are sent in Azure Service Bus.
/// </summary>
public readonly record struct SendTarget
{
    public bool IsTopic { get; init; }
    public string EntityPath { get; init; }

    public static SendTarget ForQueue(string queueName) => new()
    {
        IsTopic = false,
        EntityPath = queueName,
    };

    public static SendTarget ForTopic(string topicName) => new()
    {
        IsTopic = true,
        EntityPath = topicName,
    };
}
