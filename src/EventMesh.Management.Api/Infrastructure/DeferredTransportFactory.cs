using EventMesh.Abstractions.Transport;

namespace EventMesh.Management.Api.Infrastructure;

/// <summary>
/// Holds the resolved transport factory after the application is built.
/// </summary>
internal sealed class TransportFactoryHolder
{
    public IBrokerTransportFactory? Factory { get; set; }
}

/// <summary>
/// Defers transport factory resolution until the host service provider is available.
/// </summary>
internal sealed class DeferredTransportFactory : IBrokerTransportFactory
{
    private readonly TransportFactoryHolder _holder;
    private readonly string _transportName;

    public DeferredTransportFactory(TransportFactoryHolder holder, string transportName)
    {
        _holder = holder;
        _transportName = transportName;
    }

    public string TransportName => _transportName;

    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        if (_holder.Factory is null)
        {
            throw new InvalidOperationException("The transport factory has not been initialized.");
        }

        return _holder.Factory.CreateTransportAsync(settings, cancellationToken);
    }
}
