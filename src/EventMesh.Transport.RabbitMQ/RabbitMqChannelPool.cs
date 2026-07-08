using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// Pools RabbitMQ channels to reduce allocation overhead.
/// </summary>
public sealed class RabbitMqChannelPool : IAsyncDisposable
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly RabbitMqTransportOptions _options;
    private readonly SemaphoreSlim _poolLock = new(1, 1);
    private readonly List<PooledChannel> _availableChannels = [];
    private readonly HashSet<IChannel> _leasedChannels = [];
    private int _disposed;

    public RabbitMqChannelPool(
        RabbitMqConnectionManager connectionManager,
        IOptions<RabbitMqTransportOptions> options)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
    }

    public async Task<IChannel> RentAsync(
        bool publisherConfirms,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        await _poolLock.WaitAsync(cancellationToken);
        try
        {
            for (var index = _availableChannels.Count - 1; index >= 0; index--)
            {
                var pooled = _availableChannels[index];
                if (!pooled.Channel.IsOpen)
                {
                    await pooled.Channel.DisposeAsync();
                    _availableChannels.RemoveAt(index);
                    continue;
                }

                if (pooled.PublisherConfirms == publisherConfirms)
                {
                    _availableChannels.RemoveAt(index);
                    _leasedChannels.Add(pooled.Channel);
                    return pooled.Channel;
                }
            }
        }
        finally
        {
            _poolLock.Release();
        }

        var connection = await _connectionManager.GetConnectionAsync(cancellationToken);
        CreateChannelOptions? channelOptions = null;
        if (publisherConfirms)
        {
            channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true);
        }

        var channel = await connection.CreateChannelAsync(channelOptions, cancellationToken);

        await _poolLock.WaitAsync(cancellationToken);
        try
        {
            _leasedChannels.Add(channel);
            return channel;
        }
        finally
        {
            _poolLock.Release();
        }
    }

    public async ValueTask ReturnAsync(IChannel channel, bool publisherConfirms)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(channel);

        await _poolLock.WaitAsync();
        try
        {
            _leasedChannels.Remove(channel);

            if (!channel.IsOpen || _availableChannels.Count >= _options.ChannelPoolCapacity)
            {
                await channel.DisposeAsync();
                return;
            }

            _availableChannels.Add(new PooledChannel(channel, publisherConfirms));
        }
        finally
        {
            _poolLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _poolLock.WaitAsync();
        try
        {
            foreach (var pooled in _availableChannels)
            {
                await pooled.Channel.DisposeAsync();
            }

            foreach (var channel in _leasedChannels)
            {
                await channel.DisposeAsync();
            }

            _availableChannels.Clear();
            _leasedChannels.Clear();
        }
        finally
        {
            _poolLock.Release();
            _poolLock.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(RabbitMqChannelPool));
        }
    }

    private sealed record PooledChannel(IChannel Channel, bool PublisherConfirms);
}
