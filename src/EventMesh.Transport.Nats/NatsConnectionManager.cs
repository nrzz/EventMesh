using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Net;

namespace EventMesh.Transport.Nats;

/// <summary>
/// Manages the shared NATS client connection used by the transport.
/// </summary>
public sealed class NatsConnectionManager : IAsyncDisposable
{
    private readonly NatsTransportOptions _options;
    private readonly ILogger<NatsConnectionManager> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private NatsClient? _client;
    private int _disposed;

    public NatsConnectionManager(IOptions<NatsTransportOptions> options, ILogger<NatsConnectionManager> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<NatsClient> GetClientAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (_client is not null)
        {
            return _client;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_client is not null)
            {
                return _client;
            }

            var opts = BuildOptions();
            _client = new NatsClient(opts);
            await _client.ConnectAsync().ConfigureAwait(false);
            _logger.LogDebug("Connected to NATS at {Url}.", _options.Url);
            return _client;
        }
        finally
        {
            _gate.Release();
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
            await _client.DisposeAsync().ConfigureAwait(false);
            _client = null;
        }

        _gate.Dispose();
    }

    private NatsOpts BuildOptions()
    {
        var authOpts = new NatsAuthOpts();

        if (!string.IsNullOrWhiteSpace(_options.Token))
        {
            authOpts = authOpts with { Token = _options.Token };
        }

        if (!string.IsNullOrWhiteSpace(_options.Username))
        {
            authOpts = authOpts with
            {
                Username = _options.Username,
                Password = _options.Password ?? string.Empty,
            };
        }

        if (!string.IsNullOrWhiteSpace(_options.CredentialsFile))
        {
            authOpts = authOpts with { CredsFile = _options.CredentialsFile };
        }

        return NatsOpts.Default with
        {
            Url = _options.Url,
            AuthOpts = authOpts,
            RequestTimeout = TimeSpan.FromSeconds(10),
            ConnectTimeout = TimeSpan.FromSeconds(10),
        };
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(NatsConnectionManager));
        }
    }
}
