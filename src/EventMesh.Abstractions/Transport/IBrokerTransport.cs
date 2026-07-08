namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Low-level broker transport contract used by transport adapters.
/// </summary>
public interface IBrokerTransport : IAsyncDisposable
{
    /// <summary>
    /// Gets the transport name (for example, "rabbitmq" or "kafka").
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the capabilities supported by this transport instance.
    /// </summary>
    BrokerCapabilities GetCapabilities();

    /// <summary>
    /// Sends a message to the broker.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task<TransportSendResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Receives a message from the specified queue or subscription.
    /// </summary>
    /// <param name="queueOrSubscription">The queue or subscription to receive from.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task<TransportReceiveResult> ReceiveAsync(string queueOrSubscription, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges successful processing of a received message.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag returned by <see cref="ReceiveAsync"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task AcknowledgeAsync(string deliveryTag, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rejects a received message, optionally requeueing it for redelivery.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag returned by <see cref="ReceiveAsync"/>.</param>
    /// <param name="requeue">A value indicating whether the message should be requeued.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task RejectAsync(string deliveryTag, bool requeue = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or updates broker topology according to the provided definition.
    /// </summary>
    /// <param name="topology">The topology to provision.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default);
}
