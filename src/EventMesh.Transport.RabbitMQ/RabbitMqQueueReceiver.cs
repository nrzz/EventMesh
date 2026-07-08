using System.Threading.Channels;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EventMesh.Transport.RabbitMQ;

internal sealed class RabbitMqQueueReceiver : IAsyncDisposable
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly RabbitMqTransportOptions _options;
    private readonly ILogger _logger;
    private readonly string _queueName;
    private readonly SemaphoreSlim _startLock = new(1, 1);

    private IChannel? _channel;
    private AsyncEventingBasicConsumer? _consumer;
    private Channel<ReceivedDelivery>? _buffer;
    private int _started;
    private int _disposed;

    public RabbitMqQueueReceiver(
        RabbitMqConnectionManager connectionManager,
        IOptions<RabbitMqTransportOptions> options,
        ILogger logger,
        string queueName)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
        _logger = logger;
        _queueName = queueName;
    }

    public async Task EnsureStartedAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (_started == 1)
        {
            return;
        }

        await _startLock.WaitAsync(cancellationToken);
        try
        {
            if (_started == 1)
            {
                return;
            }

            var connection = await _connectionManager.GetConnectionAsync(cancellationToken);
            _channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            await _channel.BasicQosAsync(0, _options.PrefetchCount, global: false, cancellationToken);

            _buffer = Channel.CreateUnbounded<ReceivedDelivery>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
            });

            _consumer = new AsyncEventingBasicConsumer(_channel);
            _consumer.ReceivedAsync += OnReceivedAsync;

            await _channel.BasicConsumeAsync(
                _queueName,
                autoAck: false,
                _consumer,
                cancellationToken);

            _started = 1;
            _logger.LogDebug("Started RabbitMQ consumer for queue {QueueName}", _queueName);
        }
        finally
        {
            _startLock.Release();
        }
    }

    public async Task<ReceivedDelivery?> TryReceiveAsync(CancellationToken cancellationToken)
    {
        await EnsureStartedAsync(cancellationToken);

        if (_buffer is null)
        {
            return null;
        }

        if (_buffer.Reader.TryRead(out var delivery))
        {
            return delivery;
        }

        using var registration = cancellationToken.Register(static state =>
        {
            var buffer = (Channel<ReceivedDelivery>)state!;
            buffer.Writer.TryComplete();
        }, _buffer);

        try
        {
            if (await _buffer.Reader.WaitToReadAsync(cancellationToken))
            {
                if (_buffer.Reader.TryRead(out delivery))
                {
                    return delivery;
                }
            }
        }
        catch (ChannelClosedException)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        if (_consumer is not null)
        {
            _consumer.ReceivedAsync -= OnReceivedAsync;
        }

        _buffer?.Writer.TryComplete();

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        _startLock.Dispose();
    }

    private Task OnReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        if (_buffer is null || _channel is null)
        {
            return Task.CompletedTask;
        }

        var delivery = new ReceivedDelivery
        {
            Channel = _channel,
            AmqpDeliveryTag = args.DeliveryTag,
            Redelivered = args.Redelivered,
            Exchange = args.Exchange,
            RoutingKey = args.RoutingKey,
            Properties = args.BasicProperties,
            Body = args.Body,
        };

        if (!_buffer.Writer.TryWrite(delivery))
        {
            _logger.LogWarning(
                "Dropped RabbitMQ delivery for queue {QueueName} because the receive buffer is unavailable.",
                _queueName);
        }

        return Task.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(RabbitMqQueueReceiver));
        }
    }
}

internal sealed class ReceivedDelivery
{
    public required IChannel Channel { get; init; }

    public ulong AmqpDeliveryTag { get; init; }

    public bool Redelivered { get; init; }

    public required string Exchange { get; init; }

    public required string RoutingKey { get; init; }

    public required IReadOnlyBasicProperties Properties { get; init; }

    public required ReadOnlyMemory<byte> Body { get; init; }
}

internal sealed class InFlightDelivery
{
    public required IChannel Channel { get; init; }

    public ulong AmqpDeliveryTag { get; init; }

    public required string DeliveryTag { get; init; }

    public required TransportMessage Message { get; init; }

    public required string QueueName { get; init; }

    public string? DeadLetterDestination { get; init; }
}
