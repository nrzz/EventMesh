namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Creates configured <see cref="IBrokerTransport"/> instances for a specific broker.
/// </summary>
public interface IBrokerTransportFactory
{
    /// <summary>
    /// Gets the transport name produced by this factory.
    /// </summary>
    string TransportName { get; }

    /// <summary>
    /// Creates a new transport instance.
    /// </summary>
    /// <param name="settings">Optional transport-specific connection settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default);
}
