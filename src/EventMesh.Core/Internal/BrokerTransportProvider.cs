using EventMesh.Abstractions.Transport;

namespace EventMesh.Core.Internal;

/// <summary>
/// Lazily initializes and provides access to the configured broker transport.
/// </summary>
internal sealed class BrokerTransportProvider : IAsyncDisposable
{
    private readonly IBrokerTransportFactory _factory;
    private readonly IReadOnlyDictionary<string, string>? _settings;
    private readonly SemaphoreSlim _initializationLock = new(1, 1);
    private IBrokerTransport? _transport;
    private bool _disposed;

    public BrokerTransportProvider(
        IBrokerTransportFactory factory,
        IReadOnlyDictionary<string, string>? settings)
    {
        _factory = factory;
        _settings = settings;
    }

    public async Task<IBrokerTransport> GetTransportAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_transport is not null)
        {
            return _transport;
        }

        await _initializationLock.WaitAsync(cancellationToken);

        try
        {
            _transport ??= await _factory.CreateTransportAsync(_settings, cancellationToken);
            return _transport;
        }
        finally
        {
            _initializationLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        if (_transport is not null)
        {
            await _transport.DisposeAsync();
        }

        _initializationLock.Dispose();
        _disposed = true;
    }
}

/// <summary>
/// Resolves <see cref="IBrokerTransport"/> from the transport provider for DI consumers.
/// </summary>
internal sealed class BrokerTransportAccessor : IBrokerTransport
{
    private readonly BrokerTransportProvider _provider;

    public BrokerTransportAccessor(BrokerTransportProvider provider)
    {
        _provider = provider;
    }

    public string Name => GetTransport().Name;

    public BrokerCapabilities GetCapabilities() => GetTransport().GetCapabilities();

    public Task<TransportSendResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default) =>
        GetTransport().SendAsync(message, cancellationToken);

    public Task<TransportReceiveResult> ReceiveAsync(string queueOrSubscription, CancellationToken cancellationToken = default) =>
        GetTransport().ReceiveAsync(queueOrSubscription, cancellationToken);

    public Task AcknowledgeAsync(string deliveryTag, CancellationToken cancellationToken = default) =>
        GetTransport().AcknowledgeAsync(deliveryTag, cancellationToken);

    public Task RejectAsync(string deliveryTag, bool requeue = false, CancellationToken cancellationToken = default) =>
        GetTransport().RejectAsync(deliveryTag, requeue, cancellationToken);

    public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default) =>
        GetTransport().CreateTopologyAsync(topology, cancellationToken);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private IBrokerTransport GetTransport() =>
        _provider.GetTransportAsync().GetAwaiter().GetResult();
}
