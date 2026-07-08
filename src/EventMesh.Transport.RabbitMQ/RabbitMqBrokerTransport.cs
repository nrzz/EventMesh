using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// RabbitMQ <see cref="IBrokerTransport"/> with connection management, channel pooling,
/// publisher confirms, and manual consumer acknowledgements.
/// </summary>
public sealed class RabbitMqBrokerTransport : IBrokerTransport
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly RabbitMqChannelPool _channelPool;
    private readonly RabbitMqTopologyManager _topologyManager;
    private readonly RabbitMqMessageArchive _messageArchive;
    private readonly RabbitMqTransportOptions _options;
    private readonly ILogger<RabbitMqBrokerTransport> _logger;
    private readonly ConcurrentDictionary<string, RabbitMqQueueReceiver> _receivers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, InFlightDelivery> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, int> _deliveryCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly RabbitMqDelayedMessageScheduler _delayedScheduler;
    private readonly Task _initialization;
    private int _disposed;

    public RabbitMqBrokerTransport(
        RabbitMqConnectionManager connectionManager,
        RabbitMqChannelPool channelPool,
        IOptions<RabbitMqTransportOptions> options,
        ILogger<RabbitMqBrokerTransport> logger,
        ILoggerFactory loggerFactory)
    {
        _connectionManager = connectionManager;
        _channelPool = channelPool;
        _topologyManager = new RabbitMqTopologyManager(
            channelPool,
            options,
            loggerFactory.CreateLogger<RabbitMqTopologyManager>());
        _messageArchive = new RabbitMqMessageArchive();
        _options = options.Value;
        _logger = logger;
        _delayedScheduler = new RabbitMqDelayedMessageScheduler(
            options,
            loggerFactory.CreateLogger<RabbitMqDelayedMessageScheduler>(),
            PublishImmediateAsync);
        _initialization = InitializeAsync();
    }

    /// <inheritdoc />
    public string Name => "rabbitmq";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities()
    {
        var capabilities = BrokerCapabilities.RoutingKeys
            | BrokerCapabilities.DeadLettering
            | BrokerCapabilities.Priority
            | BrokerCapabilities.Ttl
            | BrokerCapabilities.PublisherConfirms
            | BrokerCapabilities.TopologyProvisioning
            | BrokerCapabilities.Queues
            | BrokerCapabilities.PubSub
            | BrokerCapabilities.RequestResponse
            | BrokerCapabilities.MessageHeaders
            | BrokerCapabilities.MessagePersistence
            | BrokerCapabilities.Replay
            | BrokerCapabilities.ConsumerGroups;

        capabilities |= BrokerCapabilities.DelayedDelivery;

        if (_topologyManager.DelayedPluginAvailable)
        {
            capabilities |= BrokerCapabilities.NativeScheduling;
        }

        return capabilities;
    }

    private async Task InitializeAsync()
    {
        try
        {
            await _topologyManager.DetectDelayedPluginAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Deferred RabbitMQ delayed plugin detection failed during initialization.");
        }
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await _initialization.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TransportSendResult> SendAsync(
        TransportMessage message,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken);

        if (string.IsNullOrWhiteSpace(message.Destination))
        {
            return TransportSendResult.Failure("Destination is required.");
        }

        message.MessageId ??= Guid.NewGuid().ToString("N");

        if (message.ScheduledAt is not null && message.ScheduledAt > DateTimeOffset.UtcNow)
        {
            if (_topologyManager.DelayedPluginAvailable)
            {
                return await PublishDelayedWithPluginAsync(message, cancellationToken);
            }

            if (_delayedScheduler.TrySchedule(message, cancellationToken))
            {
                _messageArchive.Record(message, message.Destination);
                return TransportSendResult.Success(message.MessageId);
            }
        }

        return await PublishImmediateAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<TransportReceiveResult> ReceiveAsync(
        string queueOrSubscription,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(queueOrSubscription);

        var queueName = _topologyManager.State.ResolveReceiveQueue(queueOrSubscription);
        var receiver = _receivers.GetOrAdd(
            queueName,
            static (name, transport) => new RabbitMqQueueReceiver(
                transport._connectionManager,
                Microsoft.Extensions.Options.Options.Create(transport._options),
                transport._logger,
                name),
            this);

        await receiver.EnsureStartedAsync(cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            var delivery = await receiver.TryReceiveAsync(cancellationToken);
            if (delivery is null)
            {
                await Task.Delay(_options.ReceivePollInterval, cancellationToken);
                continue;
            }

            var transportMessage = ToTransportMessage(delivery, queueName);
            var deliveryTag = CreateDeliveryTag(delivery.Channel, delivery.AmqpDeliveryTag);

            _inFlight[deliveryTag] = new InFlightDelivery
            {
                Channel = delivery.Channel,
                AmqpDeliveryTag = delivery.AmqpDeliveryTag,
                DeliveryTag = deliveryTag,
                Message = transportMessage,
                QueueName = queueName,
                DeadLetterDestination = _topologyManager.State.GetDeadLetterDestination(queueName),
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

        await inFlight.Channel.BasicAckAsync(inFlight.AmqpDeliveryTag, multiple: false, cancellationToken);
        _deliveryCounts.TryRemove(GetDeliveryCountKey(inFlight), out _);
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

        if (!requeue && !string.IsNullOrWhiteSpace(inFlight.DeadLetterDestination))
        {
            var deadLetterMessage = CloneForDeadLetter(inFlight.Message);
            deadLetterMessage.Destination = inFlight.DeadLetterDestination;
            deadLetterMessage.Headers["x-eventmesh-dead-letter-reason"] = "rejected-without-requeue";

            var publishResult = await PublishImmediateAsync(deadLetterMessage, cancellationToken);
            if (!publishResult.Succeeded)
            {
                throw new InvalidOperationException(
                    $"Failed to dead-letter message '{deliveryTag}': {publishResult.ErrorMessage}");
            }

            await inFlight.Channel.BasicAckAsync(inFlight.AmqpDeliveryTag, multiple: false, cancellationToken);
            _deliveryCounts.TryRemove(GetDeliveryCountKey(inFlight), out _);
            return;
        }

        if (requeue)
        {
            var deliveryCountKey = GetDeliveryCountKey(inFlight);
            _deliveryCounts.AddOrUpdate(deliveryCountKey, 1, static (_, current) => current + 1);
        }
        else
        {
            _deliveryCounts.TryRemove(GetDeliveryCountKey(inFlight), out _);
        }

        await inFlight.Channel.BasicNackAsync(
            inFlight.AmqpDeliveryTag,
            multiple: false,
            requeue,
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _topologyManager.CreateTopologyAsync(topology, cancellationToken);
    }

    /// <summary>
    /// Replays archived messages by re-publishing them to RabbitMQ.
    /// </summary>
    public Task<long> ReplayAsync(ReplayOptions options, CancellationToken cancellationToken = default) =>
        _messageArchive.ReplayAsync(PublishImmediateAsync, options, cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _delayedScheduler.DisposeAsync();

        foreach (var receiver in _receivers.Values)
        {
            await receiver.DisposeAsync();
        }

        _receivers.Clear();
        _inFlight.Clear();
        _deliveryCounts.Clear();
    }

    private async Task<TransportSendResult> PublishImmediateAsync(
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        var channel = await _channelPool.RentAsync(_options.PublisherConfirmsEnabled, cancellationToken);

        try
        {
            var (exchange, routingKey) = ResolvePublishTarget(message);
            var properties = CreateBasicProperties(message);

            using var confirmCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (_options.PublisherConfirmsEnabled)
            {
                confirmCts.CancelAfter(_options.PublisherConfirmTimeout);
            }

            await channel.BasicPublishAsync(
                exchange,
                routingKey,
                mandatory: false,
                properties,
                message.Body,
                confirmCts.Token);

            _messageArchive.Record(message, message.Destination);
            return TransportSendResult.Success(message.MessageId);
        }
        catch (Exception ex) when (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish RabbitMQ message to {Destination}", message.Destination);
            return TransportSendResult.Failure(ex.Message);
        }
        finally
        {
            await _channelPool.ReturnAsync(channel, _options.PublisherConfirmsEnabled);
        }
    }

    private async Task<TransportSendResult> PublishDelayedWithPluginAsync(
        TransportMessage message,
        CancellationToken cancellationToken)
    {
        var delay = message.ScheduledAt!.Value - DateTimeOffset.UtcNow;
        if (delay < TimeSpan.Zero)
        {
            delay = TimeSpan.Zero;
        }

        var channel = await _channelPool.RentAsync(_options.PublisherConfirmsEnabled, cancellationToken);

        try
        {
            var properties = CreateBasicProperties(message);
            properties.Headers ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            properties.Headers["x-delay"] = (int)Math.Ceiling(delay.TotalMilliseconds);

            var routingKey = _topologyManager.State.IsTopic(message.Destination)
                ? message.RoutingKey ?? string.Empty
                : message.Destination;

            using var confirmCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (_options.PublisherConfirmsEnabled)
            {
                confirmCts.CancelAfter(_options.PublisherConfirmTimeout);
            }

            await channel.BasicPublishAsync(
                _options.DelayedExchangeName,
                routingKey,
                mandatory: false,
                properties,
                message.Body,
                confirmCts.Token);

            _messageArchive.Record(message, message.Destination);
            return TransportSendResult.Success(message.MessageId);
        }
        catch (Exception ex) when (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish delayed RabbitMQ message to {Destination}", message.Destination);
            return TransportSendResult.Failure(ex.Message);
        }
        finally
        {
            await _channelPool.ReturnAsync(channel, _options.PublisherConfirmsEnabled);
        }
    }

    private (string Exchange, string RoutingKey) ResolvePublishTarget(TransportMessage message)
    {
        if (_topologyManager.State.IsTopic(message.Destination))
        {
            return (message.Destination, message.RoutingKey ?? string.Empty);
        }

        return (string.Empty, message.Destination);
    }

    private BasicProperties CreateBasicProperties(TransportMessage message)
    {
        var properties = new BasicProperties
        {
            MessageId = message.MessageId,
            ContentType = message.ContentType,
            CorrelationId = message.CorrelationId,
            ReplyTo = message.ReplyTo,
            DeliveryMode = DeliveryModes.Persistent,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
        };

        if (message.Priority is not null)
        {
            properties.Priority = (byte)Math.Clamp(message.Priority.Value, 0, _options.MaxPriority);
        }

        if (message.TimeToLive is not null)
        {
            properties.Expiration = ((long)message.TimeToLive.Value.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
        }

        if (message.Headers.Count > 0)
        {
            properties.Headers = message.Headers.ToDictionary(
                static pair => pair.Key,
                static pair => (object?)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        return properties;
    }

    private TransportMessage ToTransportMessage(ReceivedDelivery delivery, string queueName)
    {
        var headers = ExtractHeaders(delivery.Properties.Headers);
        var messageId = delivery.Properties.MessageId ?? Guid.NewGuid().ToString("N");
        var deliveryCountKey = $"{queueName}:{messageId}";

        var deliveryCount = 0;
        if (delivery.Redelivered)
        {
            deliveryCount = _deliveryCounts.TryGetValue(deliveryCountKey, out var count) ? count : 1;
        }

        return new TransportMessage
        {
            MessageId = messageId,
            Destination = queueName,
            RoutingKey = delivery.RoutingKey,
            Body = delivery.Body.ToArray(),
            ContentType = delivery.Properties.ContentType,
            Headers = headers,
            CorrelationId = delivery.Properties.CorrelationId,
            ReplyTo = delivery.Properties.ReplyTo,
            Priority = delivery.Properties.IsPriorityPresent() ? delivery.Properties.Priority : null,
            DeliveryCount = deliveryCount,
            EnqueuedAt = delivery.Properties.IsTimestampPresent()
                ? DateTimeOffset.FromUnixTimeSeconds(delivery.Properties.Timestamp.UnixTime)
                : DateTimeOffset.UtcNow,
        };
    }

    private static Dictionary<string, string> ExtractHeaders(IDictionary<string, object?>? headers)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (headers is null)
        {
            return result;
        }

        foreach (var header in headers)
        {
            result[header.Key] = ConvertHeaderValue(header.Value);
        }

        return result;
    }

    private static string ConvertHeaderValue(object? value) => value switch
    {
        null => string.Empty,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
    };

    private static string CreateDeliveryTag(IChannel channel, ulong amqpDeliveryTag) =>
        $"{channel.ChannelNumber}:{amqpDeliveryTag}";

    private static string GetDeliveryCountKey(InFlightDelivery inFlight) =>
        $"{inFlight.QueueName}:{inFlight.Message.MessageId}";

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
            throw new ObjectDisposedException(nameof(RabbitMqBrokerTransport));
        }
    }
}
