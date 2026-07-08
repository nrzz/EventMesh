using System.Collections.Concurrent;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// Schedules delayed publishes when the delayed message exchange plugin is unavailable.
/// </summary>
internal sealed class RabbitMqDelayedMessageScheduler : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, DelayedPublish> _pending = new(StringComparer.OrdinalIgnoreCase);
    private readonly RabbitMqTransportOptions _options;
    private readonly ILogger<RabbitMqDelayedMessageScheduler> _logger;
    private readonly Timer _timer;
    private readonly Func<TransportMessage, CancellationToken, Task<TransportSendResult>> _publishAsync;
    private int _disposed;

    public RabbitMqDelayedMessageScheduler(
        IOptions<RabbitMqTransportOptions> options,
        ILogger<RabbitMqDelayedMessageScheduler> logger,
        Func<TransportMessage, CancellationToken, Task<TransportSendResult>> publishAsync)
    {
        _options = options.Value;
        _logger = logger;
        _publishAsync = publishAsync;
        _timer = new Timer(PromoteDelayedMessages, null, _options.DelayCheckInterval, _options.DelayCheckInterval);
    }

    public bool TrySchedule(TransportMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (message.ScheduledAt is null || message.ScheduledAt <= DateTimeOffset.UtcNow)
        {
            return false;
        }

        var scheduleId = Guid.NewGuid().ToString("N");
        _pending[scheduleId] = new DelayedPublish
        {
            Message = CloneMessage(message),
            PublishAt = message.ScheduledAt.Value,
        };

        return true;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _timer.Dispose();
        _pending.Clear();
        await Task.CompletedTask;
    }

    private async void PromoteDelayedMessages(object? state)
    {
        if (_disposed == 1 || _pending.IsEmpty)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow;
        foreach (var entry in _pending)
        {
            if (entry.Value.PublishAt > now)
            {
                continue;
            }

            if (!_pending.TryRemove(entry.Key, out var delayed))
            {
                continue;
            }

            try
            {
                var message = delayed.Message;
                message.ScheduledAt = null;
                var result = await _publishAsync(message, CancellationToken.None);
                if (!result.Succeeded)
                {
                    _logger.LogWarning(
                        "Delayed publish to {Destination} failed: {Error}",
                        message.Destination,
                        result.ErrorMessage);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish delayed RabbitMQ message.");
            }
        }
    }

    private static TransportMessage CloneMessage(TransportMessage source) => new()
    {
        MessageId = source.MessageId ?? Guid.NewGuid().ToString("N"),
        Destination = source.Destination,
        RoutingKey = source.RoutingKey,
        Body = source.Body.ToArray(),
        ContentType = source.ContentType,
        Headers = new Dictionary<string, string>(source.Headers, StringComparer.OrdinalIgnoreCase),
        CorrelationId = source.CorrelationId,
        ReplyTo = source.ReplyTo,
        Priority = source.Priority,
        TimeToLive = source.TimeToLive,
        ScheduledAt = source.ScheduledAt,
        SessionId = source.SessionId,
        PartitionKey = source.PartitionKey,
    };

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(RabbitMqDelayedMessageScheduler));
        }
    }

    private sealed class DelayedPublish
    {
        public required TransportMessage Message { get; init; }

        public DateTimeOffset PublishAt { get; init; }
    }
}
