using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// Declares RabbitMQ exchanges, queues, bindings, and dead-letter topology.
/// </summary>
internal sealed class RabbitMqTopologyManager
{
    private const string DelayedProbeExchangeName = "eventmesh.delayed.plugin.probe";

    private readonly RabbitMqChannelPool _channelPool;
    private readonly RabbitMqTransportOptions _options;
    private readonly ILogger<RabbitMqTopologyManager> _logger;
    private readonly SemaphoreSlim _topologyLock = new(1, 1);

    private bool _delayedPluginDetected;
    private bool _delayedPluginDetectionCompleted;

    public RabbitMqTopologyManager(
        RabbitMqChannelPool channelPool,
        IOptions<RabbitMqTransportOptions> options,
        ILogger<RabbitMqTopologyManager> logger)
    {
        _channelPool = channelPool;
        _options = options.Value;
        _logger = logger;
    }

    public RabbitMqTopologyState State { get; } = new();

    public bool DelayedPluginAvailable => _delayedPluginDetected;

    public async Task DetectDelayedPluginAsync(CancellationToken cancellationToken)
    {
        if (_delayedPluginDetectionCompleted)
        {
            return;
        }

        await _topologyLock.WaitAsync(cancellationToken);
        try
        {
            if (_delayedPluginDetectionCompleted)
            {
                return;
            }

            var channel = await _channelPool.RentAsync(publisherConfirms: false, cancellationToken);
            try
            {
                await EnsureDelayedPluginDetectedAsync(channel, cancellationToken);
            }
            finally
            {
                await _channelPool.ReturnAsync(channel, publisherConfirms: false);
            }
        }
        finally
        {
            _topologyLock.Release();
        }
    }

    public async Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topology);
        cancellationToken.ThrowIfCancellationRequested();

        await _topologyLock.WaitAsync(cancellationToken);
        try
        {
            var channel = await _channelPool.RentAsync(publisherConfirms: false, cancellationToken);

            try
            {
                await EnsureDelayedPluginDetectedAsync(channel, cancellationToken);

                if (topology.ReplaceExisting)
                {
                    await DeleteTopologyAsync(channel, topology, cancellationToken);
                    State.Clear();
                }

                foreach (var topic in topology.Topics)
                {
                    var exchangeType = ResolveExchangeType(topic);
                    var arguments = ToRabbitArguments(topic.Arguments);

                    await channel.ExchangeDeclareAsync(
                        topic.Name,
                        exchangeType,
                        topic.Durable,
                        topic.AutoDelete,
                        arguments,
                        cancellationToken: cancellationToken);

                    State.RegisterTopic(topic.Name, exchangeType);
                }

                foreach (var queue in topology.Queues)
                {
                    var arguments = ToRabbitArguments(queue.Arguments);

                    if (!string.IsNullOrWhiteSpace(queue.DeadLetterDestination))
                    {
                        arguments["x-dead-letter-exchange"] = string.Empty;
                        arguments["x-dead-letter-routing-key"] = queue.DeadLetterDestination;
                    }

                    if (queue.DefaultTimeToLive is not null)
                    {
                        arguments["x-message-ttl"] = (int)queue.DefaultTimeToLive.Value.TotalMilliseconds;
                    }

                    if (queue.MaxLength is not null)
                    {
                        arguments["x-max-length"] = queue.MaxLength.Value;
                    }

                    if (queue.MaxSizeBytes is not null)
                    {
                        arguments["x-max-length-bytes"] = queue.MaxSizeBytes.Value;
                    }

                    arguments.TryAdd("x-max-priority", _options.MaxPriority);

                    await channel.QueueDeclareAsync(
                        queue.Name,
                        queue.Durable,
                        queue.Exclusive,
                        queue.AutoDelete,
                        arguments,
                        cancellationToken: cancellationToken);

                    State.RegisterQueue(queue.Name, queue.DeadLetterDestination);

                    if (_delayedPluginDetected)
                    {
                        await EnsureDelayedExchangeAsync(channel, cancellationToken);
                        await channel.QueueBindAsync(
                            queue.Name,
                            _options.DelayedExchangeName,
                            queue.Name,
                            cancellationToken: cancellationToken);
                    }
                }

                foreach (var subscription in topology.Subscriptions)
                {
                    var destination = ResolveSubscriptionDestination(subscription);
                    var routingKey = string.IsNullOrWhiteSpace(subscription.Filter) ? "#" : subscription.Filter;

                    await channel.QueueBindAsync(
                        destination,
                        subscription.Topic,
                        routingKey,
                        ToRabbitArguments(subscription.Arguments),
                        cancellationToken: cancellationToken);

                    State.RegisterSubscription(subscription.Name, destination);
                }
            }
            finally
            {
                await _channelPool.ReturnAsync(channel, publisherConfirms: false);
            }
        }
        finally
        {
            _topologyLock.Release();
        }
    }

    private async Task EnsureDelayedPluginDetectedAsync(IChannel channel, CancellationToken cancellationToken)
    {
        if (_delayedPluginDetectionCompleted)
        {
            return;
        }

        try
        {
            var arguments = new Dictionary<string, object?>
            {
                ["x-delayed-type"] = "direct",
            };

            await channel.ExchangeDeclareAsync(
                DelayedProbeExchangeName,
                "x-delayed-message",
                durable: false,
                autoDelete: true,
                arguments,
                cancellationToken: cancellationToken);

            await channel.ExchangeDeleteAsync(
                DelayedProbeExchangeName,
                ifUnused: false,
                cancellationToken: cancellationToken);

            _delayedPluginDetected = true;
            _logger.LogInformation("RabbitMQ delayed message exchange plugin detected.");
        }
        catch (OperationInterruptedException ex)
        {
            _delayedPluginDetected = false;
            _logger.LogInformation(
                ex,
                "RabbitMQ delayed message exchange plugin not available; using scheduler fallback.");
        }
        finally
        {
            _delayedPluginDetectionCompleted = true;
        }
    }

    private async Task EnsureDelayedExchangeAsync(IChannel channel, CancellationToken cancellationToken)
    {
        var arguments = new Dictionary<string, object?>
        {
            ["x-delayed-type"] = "direct",
        };

        await channel.ExchangeDeclareAsync(
            _options.DelayedExchangeName,
            "x-delayed-message",
            durable: true,
            autoDelete: false,
            arguments,
            cancellationToken: cancellationToken);
    }

    private static async Task DeleteTopologyAsync(
        IChannel channel,
        TopologyDefinition topology,
        CancellationToken cancellationToken)
    {
        foreach (var subscription in topology.Subscriptions)
        {
            var destination = ResolveSubscriptionDestination(subscription);
            var routingKey = string.IsNullOrWhiteSpace(subscription.Filter) ? "#" : subscription.Filter;

            try
            {
                await channel.QueueUnbindAsync(
                    destination,
                    subscription.Topic,
                    routingKey,
                    cancellationToken: cancellationToken);
            }
            catch (OperationInterruptedException)
            {
            }
        }

        foreach (var queue in topology.Queues)
        {
            try
            {
                await channel.QueueDeleteAsync(queue.Name, ifUnused: false, ifEmpty: false, cancellationToken);
            }
            catch (OperationInterruptedException)
            {
            }
        }

        foreach (var topic in topology.Topics)
        {
            try
            {
                await channel.ExchangeDeleteAsync(topic.Name, ifUnused: false, cancellationToken: cancellationToken);
            }
            catch (OperationInterruptedException)
            {
            }
        }
    }

    private static string ResolveExchangeType(TopicDefinition topic) =>
        string.IsNullOrWhiteSpace(topic.Type)
            ? "topic"
            : topic.Type;

    private static string ResolveSubscriptionDestination(SubscriptionDefinition subscription) =>
        subscription.Destination ?? subscription.Name;

    private static Dictionary<string, object?> ToRabbitArguments(IDictionary<string, string> arguments)
    {
        var rabbitArguments = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (var argument in arguments)
        {
            rabbitArguments[argument.Key] = argument.Value;
        }

        return rabbitArguments;
    }
}

internal sealed class RabbitMqTopologyState
{
    private readonly HashSet<string> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string?> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string> _subscriptions = new(StringComparer.OrdinalIgnoreCase);

    public void Clear()
    {
        _topics.Clear();
        _queues.Clear();
        _subscriptions.Clear();
    }

    public void RegisterTopic(string name, string exchangeType) => _topics.Add(name);

    public void RegisterQueue(string name, string? deadLetterDestination) =>
        _queues[name] = deadLetterDestination;

    public void RegisterSubscription(string name, string destination) =>
        _subscriptions[name] = destination;

    public bool IsTopic(string destination) => _topics.Contains(destination);

    public string ResolveReceiveQueue(string queueOrSubscription) =>
        _subscriptions.TryGetValue(queueOrSubscription, out var destination)
            ? destination
            : queueOrSubscription;

    public string? GetDeadLetterDestination(string queueName) =>
        _queues.TryGetValue(queueName, out var destination) ? destination : null;
}
