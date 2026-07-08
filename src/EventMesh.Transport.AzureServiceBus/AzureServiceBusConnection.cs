using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AzureServiceBus;

/// <summary>
/// Manages shared Azure Service Bus client connections.
/// </summary>
public sealed class AzureServiceBusConnection : IAsyncDisposable
{
    private readonly ILogger<AzureServiceBusConnection> _logger;
    private readonly SemaphoreSlim _initializationLock = new(1, 1);
    private ServiceBusClient? _client;
    private ServiceBusAdministrationClient? _administrationClient;
    private string? _connectionString;
    private int _disposed;

    public AzureServiceBusConnection(
        IOptions<AzureServiceBusTransportOptions> options,
        ILogger<AzureServiceBusConnection> logger)
    {
        _logger = logger;
        _connectionString = options.Value.ConnectionString;
    }

    public ServiceBusClient Client => EnsureClient();

    public ServiceBusAdministrationClient AdministrationClient => EnsureAdministrationClient();

    public void UpdateConnectionString(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        if (string.Equals(_connectionString, connectionString, StringComparison.Ordinal))
        {
            return;
        }

        _connectionString = connectionString;
        ResetClients();
    }

    private ServiceBusClient EnsureClient()
    {
        ThrowIfDisposed();

        if (_client is not null)
        {
            return _client;
        }

        _initializationLock.Wait();
        try
        {
            if (_client is not null)
            {
                return _client;
            }

            var connectionString = GetConnectionString();
            _client = new ServiceBusClient(connectionString);
            _logger.LogDebug("Azure Service Bus client initialized.");
            return _client;
        }
        finally
        {
            _initializationLock.Release();
        }
    }

    private ServiceBusAdministrationClient EnsureAdministrationClient()
    {
        ThrowIfDisposed();

        if (_administrationClient is not null)
        {
            return _administrationClient;
        }

        _initializationLock.Wait();
        try
        {
            if (_administrationClient is not null)
            {
                return _administrationClient;
            }

            var connectionString = GetConnectionString();
            _administrationClient = new ServiceBusAdministrationClient(connectionString);
            _logger.LogDebug("Azure Service Bus administration client initialized.");
            return _administrationClient;
        }
        finally
        {
            _initializationLock.Release();
        }
    }

    private string GetConnectionString()
    {
        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            throw new InvalidOperationException(
                "Azure Service Bus connection string is not configured. " +
                $"Set {AzureServiceBusTransportOptions.SectionName}:ConnectionString or call AddAzureServiceBusTransport().");
        }

        return _connectionString;
    }

    private void ResetClients()
    {
        _initializationLock.Wait();
        try
        {
            _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _client = null;
            _administrationClient = null;
        }
        finally
        {
            _initializationLock.Release();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(AzureServiceBusConnection));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        if (_client is not null)
        {
            await _client.DisposeAsync();
            _client = null;
        }

        _administrationClient = null;
    }
}
