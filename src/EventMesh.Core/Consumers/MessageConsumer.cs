using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Transport;
using EventMesh.Core.RequestResponse;
using Microsoft.Extensions.Logging;

namespace EventMesh.Core.Consumers;

/// <summary>
/// Active message consumer that receives and processes messages through the consume pipeline.
/// </summary>
internal sealed class MessageConsumer<T> : IMessageConsumer where T : notnull
{
    private readonly IBrokerTransport _transport;
    private readonly IFilterPipeline _pipeline;
    private readonly Func<ConsumeContext<T>, CancellationToken, Task> _terminal;
    private readonly RequestResponseManager _requestResponseManager;
    private readonly ILogger<MessageConsumer<T>> _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);

    private CancellationTokenSource? _receiveCts;
    private Task? _receiveTask;
    private bool _disposed;

    public MessageConsumer(
        string consumerId,
        string subscription,
        SubscribeOptions options,
        IBrokerTransport transport,
        IFilterPipeline pipeline,
        Func<ConsumeContext<T>, CancellationToken, Task> terminal,
        RequestResponseManager requestResponseManager,
        ILogger<MessageConsumer<T>> logger)
    {
        ConsumerId = consumerId;
        Subscription = subscription;
        Options = options;
        _transport = transport;
        _pipeline = pipeline;
        _terminal = terminal;
        _requestResponseManager = requestResponseManager;
        _logger = logger;
    }

    public string ConsumerId { get; }

    public string Subscription { get; }

    public SubscribeOptions Options { get; }

    public bool IsRunning { get; private set; }

    public bool IsPaused { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lifecycleLock.WaitAsync(cancellationToken);

        try
        {
            if (IsRunning)
            {
                return;
            }

            _receiveCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _receiveTask = ReceiveLoopAsync(_receiveCts.Token);
            IsRunning = true;
            IsPaused = false;
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!IsRunning)
        {
            return;
        }

        await _lifecycleLock.WaitAsync(cancellationToken);

        try
        {
            if (!IsRunning)
            {
                return;
            }

            _receiveCts?.Cancel();

            if (_receiveTask is not null)
            {
                try
                {
                    await _receiveTask.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                }
            }

            IsRunning = false;
            IsPaused = false;
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public Task PauseAsync(CancellationToken cancellationToken = default)
    {
        IsPaused = true;
        return Task.CompletedTask;
    }

    public Task ResumeAsync(CancellationToken cancellationToken = default)
    {
        IsPaused = false;
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await StopAsync();
        _receiveCts?.Dispose();
        _lifecycleLock.Dispose();
        _disposed = true;
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        var concurrency = Math.Max(1, Options.MaxConcurrency);
        var workers = Enumerable.Range(0, concurrency)
            .Select(_ => WorkerLoopAsync(cancellationToken))
            .ToArray();

        try
        {
            await Task.WhenAll(workers);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (IsPaused)
            {
                await Task.Delay(100, cancellationToken);
                continue;
            }

            TransportReceiveResult receiveResult;
            try
            {
                receiveResult = await _transport.ReceiveAsync(Subscription, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error receiving message on subscription {Subscription}.", Subscription);
                await Task.Delay(250, cancellationToken);
                continue;
            }

            if (!receiveResult.HasMessage || receiveResult.Message is null)
            {
                await Task.Delay(50, cancellationToken);
                continue;
            }

            try
            {
                await ProcessMessageAsync(receiveResult, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Unhandled error processing message on subscription {Subscription}.",
                    Subscription);
            }
        }
    }

    private async Task ProcessMessageAsync(TransportReceiveResult receiveResult, CancellationToken cancellationToken)
    {
        var transportMessage = receiveResult.Message!;
        var envelope = Internal.EnvelopeMapper.FromTransportMessage(transportMessage);

        if (envelope.CorrelationId is not null
            && envelope.Headers.TryGetValue("eventmesh-response", out var responseMarker)
            && string.Equals(responseMarker, "true", StringComparison.OrdinalIgnoreCase))
        {
            if (envelope.Data is { Length: > 0 }
                && await _requestResponseManager.TryCompleteAsync(
                    envelope.CorrelationId,
                    envelope.Data.Value,
                    envelope.DataContentType,
                    cancellationToken))
            {
                if (!string.IsNullOrWhiteSpace(receiveResult.DeliveryTag))
                {
                    await _transport.AcknowledgeAsync(receiveResult.DeliveryTag, cancellationToken);
                }

                return;
            }
        }

        var context = new ConsumeContext<T>
        {
            Message = default!,
            Envelope = envelope,
            Options = Options,
            DeliveryTag = receiveResult.DeliveryTag,
            DeliveryCount = transportMessage.DeliveryCount,
            CancellationToken = cancellationToken,
        };

        await _pipeline.ConsumeAsync(context, TerminalAsync, cancellationToken);

        if (Options.AutoAcknowledge
            && !context.IsAcknowledged
            && !context.IsRejected
            && !string.IsNullOrWhiteSpace(context.DeliveryTag))
        {
            await _transport.AcknowledgeAsync(context.DeliveryTag, cancellationToken);
            context.IsAcknowledged = true;
        }
    }

    private Task TerminalAsync(ConsumeContext<T> context, CancellationToken cancellationToken) =>
        _terminal(context, cancellationToken);
}
