using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Azure.Messaging.ServiceBus;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AzureServiceBus;

/// <summary>
/// Azure Service Bus <see cref="IBrokerTransport"/> with sessions, native scheduling,
/// transactions, and dead-letter support.
/// </summary>
public sealed class AzureServiceBusBrokerTransport : IBrokerTransport
{
    internal const string DeadLetterReasonHeader = "x-eventmesh-dead-letter-reason";
    private const string RoutingKeyApplicationProperty = "routingKey";

    private readonly AzureServiceBusConnection _connection;
    private readonly AzureServiceBusTopologyManager _topologyManager;
    private readonly AzureServiceBusTransportOptions _options;
    private readonly ILogger<AzureServiceBusBrokerTransport> _logger;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, ReceiverSession> _receivers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, InFlightMessage> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly AzureServiceBusMessageArchive _messageArchive = new();
    private int _disposed;

    public AzureServiceBusBrokerTransport(
        AzureServiceBusConnection connection,
        AzureServiceBusTopologyManager topologyManager,
        IOptions<AzureServiceBusTransportOptions> options,
        ILogger<AzureServiceBusBrokerTransport> logger)
    {
        _connection = connection;
        _topologyManager = topologyManager;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public string Name => "azureservicebus";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities() =>
        BrokerCapabilities.Sessions
        | BrokerCapabilities.Transactions
        | BrokerCapabilities.NativeScheduling
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.Ttl
        | BrokerCapabilities.TopologyProvisioning
        | BrokerCapabilities.MessageHeaders
        | BrokerCapabilities.MessagePersistence
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.Queues
        | BrokerCapabilities.RequestResponse
        | BrokerCapabilities.SubscriptionFilters
        | BrokerCapabilities.Ordering
        | BrokerCapabilities.Fifo;

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

        try
        {
            var sendTarget = _topologyManager.ResolveSendTarget(message.Destination);
            var sender = GetOrCreateSender(sendTarget.EntityPath);
            var serviceBusMessage = ToServiceBusMessage(message);

            await sender.SendMessageAsync(serviceBusMessage, cancellationToken);

            _messageArchive.Record(message, message.Destination);
            return TransportSendResult.Success(message.MessageId);
        }
        catch (Exception ex) when (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send Azure Service Bus message to {Destination}", message.Destination);
            return TransportSendResult.Failure(ex.Message);
        }
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
        var receiverSession = _receivers.GetOrAdd(
            BuildReceiverKey(receiveTarget),
            static (_, args) => new ReceiverSession(args.Connection, args.Options, args.Logger),
            new ReceiverSessionArgs(_connection, _options, _logger));

        while (!cancellationToken.IsCancellationRequested)
        {
            var receivedMessage = await receiverSession.ReceiveAsync(receiveTarget, cancellationToken);
            if (receivedMessage is null)
            {
                await Task.Delay(_options.ReceivePollInterval, cancellationToken);
                continue;
            }

            var transportMessage = ToTransportMessage(receivedMessage, receiveTarget);
            var deliveryTag = receivedMessage.LockToken;

            _inFlight[deliveryTag] = new InFlightMessage
            {
                DeliveryTag = deliveryTag,
                ReceivedMessage = receivedMessage,
                ReceiverSession = receiverSession,
                ReceiveTarget = receiveTarget,
                Message = transportMessage,
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

        await inFlight.ReceiverSession.CompleteAsync(inFlight.ReceivedMessage, cancellationToken);
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
            await inFlight.ReceiverSession.AbandonAsync(inFlight.ReceivedMessage, cancellationToken);
            return;
        }

        var deadLetterProperties = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
        {
            [DeadLetterReasonHeader] = "rejected-without-requeue",
        };

        await inFlight.ReceiverSession.DeadLetterAsync(
            inFlight.ReceivedMessage,
            deadLetterProperties,
            deadLetterReason: "rejected-without-requeue",
            deadLetterErrorDescription: "Message rejected without requeue.",
            cancellationToken);
    }

    /// <inheritdoc />
    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default) =>
        _topologyManager.CreateTopologyAsync(topology, cancellationToken);

    /// <summary>
    /// Replays archived messages by re-publishing them to Azure Service Bus.
    /// </summary>
    public Task<long> ReplayAsync(ReplayOptions options, CancellationToken cancellationToken = default) =>
        _messageArchive.ReplayAsync(SendAsync, options, cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        foreach (var sender in _senders.Values)
        {
            await sender.DisposeAsync();
        }

        _senders.Clear();

        foreach (var receiver in _receivers.Values)
        {
            await receiver.DisposeAsync();
        }

        _receivers.Clear();
        _inFlight.Clear();
    }

    private ServiceBusSender GetOrCreateSender(string entityPath) =>
        _senders.GetOrAdd(
            entityPath,
            static (path, connection) => connection.Client.CreateSender(path),
            _connection);

    private static string BuildReceiverKey(ReceiveTarget receiveTarget) =>
        receiveTarget.Kind switch
        {
            ReceiveTargetKind.DeadLetter => $"dlq:{receiveTarget.EntityPath}",
            ReceiveTargetKind.Subscription => $"sub:{receiveTarget.EntityPath}",
            _ => $"queue:{receiveTarget.EntityPath}:{receiveTarget.RequiresSession}",
        };

    private ServiceBusMessage ToServiceBusMessage(TransportMessage message)
    {
        var serviceBusMessage = new ServiceBusMessage(message.Body)
        {
            MessageId = message.MessageId,
            ContentType = message.ContentType,
            CorrelationId = message.CorrelationId,
            ReplyTo = message.ReplyTo,
            Subject = message.RoutingKey,
            SessionId = message.SessionId ?? message.PartitionKey,
        };

        if (message.Priority is not null)
        {
            serviceBusMessage.ApplicationProperties["priority"] = message.Priority.Value;
        }

        if (message.TimeToLive is not null)
        {
            serviceBusMessage.TimeToLive = message.TimeToLive.Value;
        }

        if (message.ScheduledAt is not null && message.ScheduledAt > DateTimeOffset.UtcNow)
        {
            serviceBusMessage.ScheduledEnqueueTime = message.ScheduledAt.Value.UtcDateTime;
        }

        if (!string.IsNullOrWhiteSpace(message.RoutingKey))
        {
            serviceBusMessage.ApplicationProperties[RoutingKeyApplicationProperty] = message.RoutingKey;
        }

        foreach (var header in message.Headers)
        {
            serviceBusMessage.ApplicationProperties[header.Key] = header.Value;
        }

        return serviceBusMessage;
    }

    private TransportMessage ToTransportMessage(ServiceBusReceivedMessage receivedMessage, ReceiveTarget receiveTarget)
    {
        var headers = ExtractHeaders(receivedMessage.ApplicationProperties);
        var routingKey = receivedMessage.Subject;

        if (string.IsNullOrWhiteSpace(routingKey)
            && receivedMessage.ApplicationProperties.TryGetValue(RoutingKeyApplicationProperty, out var routingValue))
        {
            routingKey = ConvertPropertyValue(routingValue);
        }

        if (receiveTarget.Kind == ReceiveTargetKind.DeadLetter
            && !headers.ContainsKey(DeadLetterReasonHeader)
            && receivedMessage.DeadLetterReason is not null)
        {
            headers[DeadLetterReasonHeader] = receivedMessage.DeadLetterReason;
        }

        var deliveryCount = Math.Max(0, receivedMessage.DeliveryCount - 1);

        return new TransportMessage
        {
            MessageId = receivedMessage.MessageId ?? Guid.NewGuid().ToString("N"),
            Destination = receiveTarget.LogicalName,
            RoutingKey = routingKey,
            Body = receivedMessage.Body.ToArray(),
            ContentType = receivedMessage.ContentType,
            Headers = headers,
            CorrelationId = receivedMessage.CorrelationId,
            ReplyTo = receivedMessage.ReplyTo,
            SessionId = receivedMessage.SessionId,
            DeliveryCount = deliveryCount,
            EnqueuedAt = receivedMessage.EnqueuedTime,
            DeliveryTag = receivedMessage.LockToken,
        };
    }

    private static Dictionary<string, string> ExtractHeaders(IReadOnlyDictionary<string, object> applicationProperties)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var property in applicationProperties)
        {
            headers[property.Key] = ConvertPropertyValue(property.Value);
        }

        return headers;
    }

    private static string ConvertPropertyValue(object? value) => value switch
    {
        null => string.Empty,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
    };

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(AzureServiceBusBrokerTransport));
        }
    }

    private sealed class InFlightMessage
    {
        public required string DeliveryTag { get; init; }
        public required ServiceBusReceivedMessage ReceivedMessage { get; init; }
        public required ReceiverSession ReceiverSession { get; init; }
        public required ReceiveTarget ReceiveTarget { get; init; }
        public required TransportMessage Message { get; init; }
    }

    private readonly record struct ReceiverSessionArgs(
        AzureServiceBusConnection Connection,
        AzureServiceBusTransportOptions Options,
        ILogger Logger);

    private sealed class ReceiverSession : IAsyncDisposable
    {
        private readonly AzureServiceBusConnection _connection;
        private readonly AzureServiceBusTransportOptions _options;
        private readonly ILogger _logger;
        private readonly SemaphoreSlim _sessionLock = new(1, 1);
        private ServiceBusReceiver? _receiver;
        private ServiceBusSessionReceiver? _sessionReceiver;
        private string? _receiverKey;
        private int _disposed;

        public ReceiverSession(
            AzureServiceBusConnection connection,
            AzureServiceBusTransportOptions options,
            ILogger logger)
        {
            _connection = connection;
            _options = options;
            _logger = logger;
        }

        public async Task<ServiceBusReceivedMessage?> ReceiveAsync(
            ReceiveTarget receiveTarget,
            CancellationToken cancellationToken)
        {
            await EnsureReceiverAsync(receiveTarget, cancellationToken);

            if (_sessionReceiver is not null)
            {
                return await _sessionReceiver.ReceiveMessageAsync(_options.ReceiveWaitTime, cancellationToken);
            }

            return await _receiver!.ReceiveMessageAsync(_options.ReceiveWaitTime, cancellationToken);
        }

        public async Task CompleteAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        {
            if (_sessionReceiver is not null)
            {
                await _sessionReceiver.CompleteMessageAsync(message, cancellationToken);
                return;
            }

            await _receiver!.CompleteMessageAsync(message, cancellationToken);
        }

        public async Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        {
            if (_sessionReceiver is not null)
            {
                await _sessionReceiver.AbandonMessageAsync(message, cancellationToken: cancellationToken);
                return;
            }

            await _receiver!.AbandonMessageAsync(message, cancellationToken: cancellationToken);
        }

        public async Task DeadLetterAsync(
            ServiceBusReceivedMessage message,
            IDictionary<string, object> propertiesToModify,
            string deadLetterReason,
            string deadLetterErrorDescription,
            CancellationToken cancellationToken)
        {
            if (_sessionReceiver is not null)
            {
                await _sessionReceiver.DeadLetterMessageAsync(
                    message,
                    propertiesToModify,
                    deadLetterReason,
                    deadLetterErrorDescription,
                    cancellationToken);
                return;
            }

            await _receiver!.DeadLetterMessageAsync(
                message,
                propertiesToModify,
                deadLetterReason,
                deadLetterErrorDescription,
                cancellationToken);
        }

        private async Task EnsureReceiverAsync(ReceiveTarget receiveTarget, CancellationToken cancellationToken)
        {
            var receiverKey = BuildReceiverInstanceKey(receiveTarget);
            if (_receiverKey == receiverKey && (_receiver is not null || _sessionReceiver is not null))
            {
                return;
            }

            await _sessionLock.WaitAsync(cancellationToken);
            try
            {
                if (_receiverKey == receiverKey && (_receiver is not null || _sessionReceiver is not null))
                {
                    return;
                }

                await DisposeReceiverAsync();

                var client = _connection.Client;
                var receiverOptions = new ServiceBusReceiverOptions
                {
                    PrefetchCount = _options.PrefetchCount,
                    ReceiveMode = ServiceBusReceiveMode.PeekLock,
                };

                if (receiveTarget.Kind == ReceiveTargetKind.DeadLetter)
                {
                    receiverOptions.SubQueue = SubQueue.DeadLetter;
                    _receiver = client.CreateReceiver(receiveTarget.EntityPath, receiverOptions);
                }
                else if (receiveTarget.RequiresSession)
                {
                    _sessionReceiver = await client.AcceptNextSessionAsync(
                        receiveTarget.EntityPath,
                        new ServiceBusSessionReceiverOptions
                        {
                            PrefetchCount = _options.PrefetchCount,
                            ReceiveMode = ServiceBusReceiveMode.PeekLock,
                        },
                        cancellationToken);
                }
                else
                {
                    _receiver = client.CreateReceiver(receiveTarget.EntityPath, receiverOptions);
                }

                _receiverKey = receiverKey;
            }
            finally
            {
                _sessionLock.Release();
            }
        }

        private static string BuildReceiverInstanceKey(ReceiveTarget receiveTarget) =>
            receiveTarget.Kind switch
            {
                ReceiveTargetKind.DeadLetter => $"dlq:{receiveTarget.EntityPath}",
                ReceiveTargetKind.Subscription => $"sub:{receiveTarget.EntityPath}",
                _ => $"queue:{receiveTarget.EntityPath}:session={receiveTarget.RequiresSession}",
            };

        private async Task DisposeReceiverAsync()
        {
            if (_receiver is not null)
            {
                await _receiver.DisposeAsync();
                _receiver = null;
            }

            if (_sessionReceiver is not null)
            {
                await _sessionReceiver.DisposeAsync();
                _sessionReceiver = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

            await _sessionLock.WaitAsync();
            try
            {
                await DisposeReceiverAsync();
            }
            finally
            {
                _sessionLock.Release();
                _sessionLock.Dispose();
            }
        }
    }
}
