using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Observability;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Scheduling;
using EventMesh.Abstractions.Transport;
using EventMesh.Core.Capabilities;
using EventMesh.Core.Consumers;
using EventMesh.Core.Internal;
using EventMesh.Core.Observability;
using EventMesh.Core.RequestResponse;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Core.Bus;

/// <summary>
/// Default <see cref="IMessageBus"/> implementation orchestrating pipeline, transport, and scheduler.
/// </summary>
public sealed class MessageBus : IMessageBus
{
    private readonly IFilterPipeline _pipeline;
    private readonly IBrokerTransport _transport;
    private readonly IMessageScheduler _scheduler;
    private readonly CapabilityEmulator _capabilityEmulator;
    private readonly RequestResponseManager _requestResponseManager;
    private readonly IConsumerManager _consumerManager;
    private readonly MessageTopicResolver _topicResolver;
    private readonly ICorrelationContext _correlationContext;
    private readonly EventMeshOptions _options;
    private readonly ILoggerFactory _loggerFactory;

    public MessageBus(
        IFilterPipeline pipeline,
        IBrokerTransport transport,
        IMessageScheduler scheduler,
        CapabilityEmulator capabilityEmulator,
        RequestResponseManager requestResponseManager,
        IConsumerManager consumerManager,
        MessageTopicResolver topicResolver,
        ICorrelationContext correlationContext,
        IOptions<EventMeshOptions> options,
        ILoggerFactory loggerFactory)
    {
        _pipeline = pipeline;
        _transport = transport;
        _scheduler = scheduler;
        _capabilityEmulator = capabilityEmulator;
        _requestResponseManager = requestResponseManager;
        _consumerManager = consumerManager;
        _topicResolver = topicResolver;
        _correlationContext = correlationContext;
        _options = options.Value;
        _loggerFactory = loggerFactory;
    }

    /// <inheritdoc />
    public Task PublishAsync<T>(
        T message,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        options ??= new PublishOptions();
        var context = new PublishContext<T>
        {
            Message = message,
            Options = options,
            Destination = _topicResolver.ResolveTopic<T>(options.Topic),
            CancellationToken = cancellationToken,
        };

        return _pipeline.PublishAsync(context, PublishTerminalAsync, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<string> ScheduleAsync<T>(
        T message,
        DateTimeOffset scheduledAt,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        options ??= new ScheduleOptions();

        if (_capabilityEmulator.ShouldUseSchedulerForDelay(_transport))
        {
            return await _scheduler.ScheduleAsync(message, scheduledAt, options, cancellationToken);
        }

        var publishOptions = options.PublishOptions ?? new PublishOptions
        {
            Topic = options.Topic,
            RoutingKey = options.RoutingKey,
            CorrelationId = options.CorrelationId,
            CausationId = options.CausationId,
            Headers = options.Headers,
        };

        await PublishAsync(message, publishOptions, cancellationToken);
        return options.ScheduleId ?? Guid.NewGuid().ToString("N");
    }

    /// <inheritdoc />
    public Task<string> ScheduleAsync<T>(
        T message,
        TimeSpan delay,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull =>
        ScheduleAsync(message, DateTimeOffset.UtcNow.Add(delay), options, cancellationToken);

    /// <inheritdoc />
    public async Task<TResponse> RequestAsync<TRequest, TResponse>(
        TRequest request,
        RequestOptions? options = null,
        CancellationToken cancellationToken = default)
        where TRequest : notnull
        where TResponse : notnull
    {
        options ??= new RequestOptions();
        var timeout = options.Timeout > TimeSpan.Zero ? options.Timeout : _options.DefaultRequestTimeout;
        var correlationId = options.CorrelationId ?? Guid.NewGuid().ToString("N");
        _requestResponseManager.Register<TResponse>(correlationId, timeout, cancellationToken);

        var replyTo = options.ReplyTo ?? _capabilityEmulator.ResolveReplyDestination(_options.ApplicationName);
        var publishOptions = new PublishOptions
        {
            Topic = options.Topic,
            RoutingKey = options.RoutingKey,
            CorrelationId = correlationId,
            CausationId = options.CausationId ?? _correlationContext.CausationId,
            ContentType = options.ContentType,
            Headers = MergeHeaders(options.Headers, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["reply-to"] = replyTo,
                ["eventmesh-request"] = "true",
            }),
            SessionId = options.SessionId,
            PartitionKey = options.PartitionKey,
        };

        await PublishAsync(request, publishOptions, cancellationToken);
        return await _requestResponseManager.WaitAsync<TResponse>(correlationId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> ReplayAsync(
        string topicOrStream,
        ReplayOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!_transport.GetCapabilities().SupportsAll(BrokerCapabilities.Replay))
        {
            throw new CapabilityNotSupportedException(
                BrokerCapabilities.Replay,
                _transport.Name,
                $"Transport '{_transport.Name}' does not support replay for topic '{topicOrStream}'.");
        }

        throw new NotSupportedException(
            "Replay is supported by the transport but no replay implementation is registered in EventMesh.Core.");
    }

    /// <inheritdoc />
    public Task<IMessageConsumer> SubscribeAsync<T>(
        Func<T, CancellationToken, Task> handler,
        SubscribeOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(handler);
        return SubscribeInternalAsync<T>(
            (message, token) => handler(message, token),
            options,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<IMessageConsumer> SubscribeAsync<T>(
        IMessageHandler<T> handler,
        SubscribeOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(handler);
        return SubscribeInternalAsync<T>(
            handler.HandleAsync,
            options,
            cancellationToken);
    }

    private Task<IMessageConsumer> SubscribeInternalAsync<T>(
        Func<T, CancellationToken, Task> handler,
        SubscribeOptions? options,
        CancellationToken cancellationToken) where T : notnull
    {
        options ??= new SubscribeOptions();
        var subscription = _topicResolver.ResolveTopic<T>(options.Topic);
        var consumer = new MessageConsumer<T>(
            Guid.NewGuid().ToString("N"),
            subscription,
            options,
            _transport,
            _pipeline,
            async (context, token) =>
            {
                await handler(context.Message, token);

                if (options.AutoAcknowledge
                    && !context.IsAcknowledged
                    && !context.IsRejected
                    && !string.IsNullOrWhiteSpace(context.DeliveryTag))
                {
                    await _transport.AcknowledgeAsync(context.DeliveryTag, token);
                    context.IsAcknowledged = true;
                }
            },
            _requestResponseManager,
            _loggerFactory.CreateLogger<MessageConsumer<T>>());

        _consumerManager.Add(consumer);
        return StartConsumerAsync(consumer, cancellationToken);
    }

    private static async Task<IMessageConsumer> StartConsumerAsync(
        IMessageConsumer consumer,
        CancellationToken cancellationToken)
    {
        if (!consumer.IsRunning)
        {
            await consumer.StartAsync(cancellationToken);
        }

        return consumer;
    }

    private async Task PublishTerminalAsync<T>(PublishContext<T> context, CancellationToken cancellationToken)
        where T : notnull
    {
        if (context.Envelope is null)
        {
            throw new InvalidOperationException("Publish pipeline must produce an envelope before transport send.");
        }

        if (string.IsNullOrWhiteSpace(context.Destination))
        {
            throw new InvalidOperationException("Publish pipeline must resolve a destination before transport send.");
        }

        var transportMessage = EnvelopeMapper.ToTransportMessage(context.Envelope, context.Destination, context.Options);
        var result = await _transport.SendAsync(transportMessage, cancellationToken);
        if (!result.Succeeded)
        {
            throw new InvalidOperationException(result.ErrorMessage ?? "Transport send failed.");
        }
    }

    private static IDictionary<string, string>? MergeHeaders(
        IDictionary<string, string>? existing,
        IDictionary<string, string> additional)
    {
        if (existing is null)
        {
            return additional;
        }

        var merged = new Dictionary<string, string>(existing, StringComparer.OrdinalIgnoreCase);
        foreach (var header in additional)
        {
            merged[header.Key] = header.Value;
        }

        return merged;
    }
}
