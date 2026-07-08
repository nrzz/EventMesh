using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.InMemory;

/// <summary>
/// In-memory <see cref="IBrokerTransport"/> implementation for testing and benchmarking.
/// </summary>
public sealed class InMemoryBrokerTransport : IBrokerTransport
{
    private readonly InMemoryBrokerState _brokerState;
    private readonly InMemoryMessageStore _messageStore;
    private readonly InMemoryTransportOptions _options;
    private int _disposed;

    public InMemoryBrokerTransport(
        InMemoryBrokerState brokerState,
        InMemoryMessageStore messageStore,
        IOptions<InMemoryTransportOptions> options)
    {
        _brokerState = brokerState;
        _messageStore = messageStore;
        _options = options.Value;
    }

    /// <inheritdoc />
    public string Name => "inmemory";

    /// <inheritdoc />
    public BrokerCapabilities GetCapabilities() =>
        BrokerCapabilities.DelayedDelivery
        | BrokerCapabilities.NativeScheduling
        | BrokerCapabilities.Priority
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.Ordering
        | BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.RoutingKeys
        | BrokerCapabilities.Fifo
        | BrokerCapabilities.RequestResponse
        | BrokerCapabilities.MessagePersistence
        | BrokerCapabilities.PublisherConfirms
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.Queues
        | BrokerCapabilities.MessageHeaders
        | BrokerCapabilities.TopologyProvisioning
        | BrokerCapabilities.SubscriptionFilters
        | BrokerCapabilities.ExponentialBackoff;

    /// <inheritdoc />
    public Task<TransportSendResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(message);

        if (string.IsNullOrWhiteSpace(message.Destination))
        {
            return Task.FromResult(TransportSendResult.Failure("Destination is required."));
        }

        var result = _brokerState.Enqueue(message);
        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public async Task<TransportReceiveResult> ReceiveAsync(
        string queueOrSubscription,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(queueOrSubscription);

        while (!cancellationToken.IsCancellationRequested)
        {
            if (_brokerState.TryDequeue(queueOrSubscription, out var message, out var deliveryTag))
            {
                return TransportReceiveResult.Received(message, deliveryTag);
            }

            await Task.Delay(_options.ReceivePollInterval, cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return TransportReceiveResult.Empty();
    }

    /// <inheritdoc />
    public Task AcknowledgeAsync(string deliveryTag, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(deliveryTag);

        if (!_brokerState.TryAcknowledge(deliveryTag))
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is not in flight.");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RejectAsync(
        string deliveryTag,
        bool requeue = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(deliveryTag);

        if (!_brokerState.TryReject(deliveryTag, requeue, out _))
        {
            throw new InvalidOperationException($"Delivery tag '{deliveryTag}' is not in flight.");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(topology);

        _brokerState.CreateTopology(topology);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Replays stored messages according to the provided options.
    /// </summary>
    public Task<long> ReplayAsync(ReplayOptions options, CancellationToken cancellationToken = default) =>
        _messageStore.ReplayAsync(_brokerState, options, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return ValueTask.CompletedTask;
        }

        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(InMemoryBrokerTransport));
        }
    }
}
