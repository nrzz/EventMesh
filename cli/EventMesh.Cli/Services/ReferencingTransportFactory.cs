using EventMesh.Abstractions.Transport;

namespace EventMesh.Cli.Services;

/// <summary>
/// Holds the resolved transport factory after the host is built.
/// </summary>
internal sealed class TransportFactoryReference
{
    public IBrokerTransportFactory? Factory { get; set; }
}

/// <summary>
/// Defers transport creation until the host service provider is available.
/// </summary>
internal sealed class ReferencingTransportFactory : IBrokerTransportFactory
{
    private readonly TransportFactoryReference _reference;

    public ReferencingTransportFactory(TransportFactoryReference reference)
    {
        _reference = reference;
    }

    public string TransportName => "inmemory";

    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        if (_reference.Factory is null)
        {
            throw new InvalidOperationException("The transport factory has not been initialized.");
        }

        return _reference.Factory.CreateTransportAsync(settings, cancellationToken);
    }
}
