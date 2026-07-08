using System.Collections.Concurrent;
using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Scheduling;
using EventMesh.Abstractions.Serialization;
using EventMesh.Core.Internal;

namespace EventMesh.Core.Scheduling;

/// <summary>
/// In-memory message scheduler for transports without native delay support.
/// </summary>
public sealed class InMemoryMessageScheduler : IMessageScheduler
{
    private readonly ConcurrentDictionary<string, ScheduledMessage> _messages = new(StringComparer.OrdinalIgnoreCase);
    private readonly MessageTopicResolver _topicResolver;
    private readonly IMessageSerializer _serializer;

    public InMemoryMessageScheduler(MessageTopicResolver topicResolver, IMessageSerializer serializer)
    {
        _topicResolver = topicResolver;
        _serializer = serializer;
    }

    /// <inheritdoc />
    public async Task<string> ScheduleAsync<T>(
        T message,
        DateTimeOffset scheduledAt,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        cancellationToken.ThrowIfCancellationRequested();
        options ??= new ScheduleOptions();
        options.ScheduledAt = scheduledAt;

        var scheduleId = options.ScheduleId ?? Guid.NewGuid().ToString("N");
        var destination = _topicResolver.ResolveTopic<T>(options.Topic);
        var contentType = _serializer.DefaultContentType;
        var data = await _serializer.SerializeAsync(message, contentType, cancellationToken);

        var builder = MessageEnvelope.Create()
            .WithId(scheduleId)
            .WithType(_topicResolver.ResolveMessageType<T>())
            .WithSource(_topicResolver.ResolveSource())
            .WithData(data)
            .WithDataContentType(contentType)
            .WithCorrelationId(options.CorrelationId)
            .WithCausationId(options.CausationId);

        if (options.Headers is not null)
        {
            builder.WithHeaders(options.Headers);
        }

        StoreEnvelope(scheduleId, builder.Build(), destination, scheduledAt, options);
        return scheduleId;
    }

    /// <inheritdoc />
    public Task<string> ScheduleAsync<T>(
        T message,
        TimeSpan delay,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull =>
        ScheduleAsync(message, DateTimeOffset.UtcNow.Add(delay), options, cancellationToken);

    /// <inheritdoc />
    public Task<bool> CancelAsync(string scheduleId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_messages.TryGetValue(scheduleId, out var scheduled))
        {
            return Task.FromResult(false);
        }

        if (scheduled.State is ScheduledMessageState.Delivered or ScheduledMessageState.Cancelled)
        {
            return Task.FromResult(false);
        }

        scheduled.State = ScheduledMessageState.Cancelled;
        scheduled.CompletedAt = DateTimeOffset.UtcNow;
        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public async Task<int> CancelGroupAsync(string scheduleGroupId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var cancelled = 0;
        foreach (var scheduled in _messages.Values.ToArray())
        {
            if (!string.Equals(scheduled.ScheduleGroupId, scheduleGroupId, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (scheduled.State is ScheduledMessageState.Pending or ScheduledMessageState.Dispatching
                && await CancelAsync(scheduled.ScheduleId, cancellationToken))
            {
                cancelled++;
            }
        }

        return cancelled;
    }

    /// <inheritdoc />
    public Task<ScheduledMessage?> GetAsync(string scheduleId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _messages.TryGetValue(scheduleId, out var scheduled);
        return Task.FromResult(scheduled);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ScheduledMessage>> GetDueAsync(
        DateTimeOffset asOf,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var due = _messages.Values
            .Where(message => message.State == ScheduledMessageState.Pending && message.ScheduledAt <= asOf)
            .OrderBy(message => message.ScheduledAt)
            .Take(batchSize)
            .ToList();

        return Task.FromResult<IReadOnlyList<ScheduledMessage>>(due);
    }

    internal void MarkDispatching(string scheduleId)
    {
        if (_messages.TryGetValue(scheduleId, out var scheduled))
        {
            scheduled.State = ScheduledMessageState.Dispatching;
        }
    }

    internal void MarkDelivered(string scheduleId)
    {
        if (_messages.TryGetValue(scheduleId, out var scheduled))
        {
            scheduled.State = ScheduledMessageState.Delivered;
            scheduled.CompletedAt = DateTimeOffset.UtcNow;
        }
    }

    internal void MarkFailed(string scheduleId)
    {
        if (_messages.TryGetValue(scheduleId, out var scheduled))
        {
            scheduled.State = ScheduledMessageState.Failed;
            scheduled.CompletedAt = DateTimeOffset.UtcNow;
        }
    }

    private void StoreEnvelope(
        string scheduleId,
        MessageEnvelope envelope,
        string destination,
        DateTimeOffset scheduledAt,
        ScheduleOptions options)
    {
        var scheduled = new ScheduledMessage
        {
            ScheduleId = scheduleId,
            Envelope = envelope,
            Destination = destination,
            ScheduledAt = scheduledAt,
            ScheduleGroupId = options.ScheduleGroupId,
            Options = options.Clone(),
            State = ScheduledMessageState.Pending,
        };

        _messages[scheduleId] = scheduled;
    }
}
