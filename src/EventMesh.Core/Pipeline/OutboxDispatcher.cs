using EventMesh.Abstractions.Reliability;
using EventMesh.Abstractions.Transport;
using EventMesh.Core.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Dispatches pending outbox messages through the broker transport.
/// </summary>
internal sealed class OutboxDispatcher : IHostedService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private const int BatchSize = 100;

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxDispatcher> _logger;
    private CancellationTokenSource? _hostCts;
    private Task? _hostTask;

    public OutboxDispatcher(IServiceScopeFactory scopeFactory, ILogger<OutboxDispatcher> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _hostCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _hostTask = RunAsync(_hostCts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_hostCts is not null)
        {
            await _hostCts.CancelAsync();
        }

        if (_hostTask is not null)
        {
            try
            {
                await _hostTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DispatchPendingAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error dispatching outbox messages.");
            }

            try
            {
                await Task.Delay(PollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task DispatchPendingAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var outboxStore = scope.ServiceProvider.GetService<IOutboxStore>();
        if (outboxStore is null)
        {
            return;
        }

        var transport = scope.ServiceProvider.GetRequiredService<IBrokerTransport>();
        var pending = await outboxStore.GetPendingAsync(BatchSize, cancellationToken);
        if (pending.Count == 0)
        {
            return;
        }

        foreach (var message in pending)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!await outboxStore.TryClaimAsync(message.Id, cancellationToken))
            {
                continue;
            }

            try
            {
                var transportMessage = EnvelopeMapper.ToTransportMessage(message.Envelope, message.Destination);
                var result = await transport.SendAsync(transportMessage, cancellationToken);
                if (!result.Succeeded)
                {
                    await outboxStore.MarkFailedAsync(
                        message.Id,
                        result.ErrorMessage ?? "Transport send failed.",
                        cancellationToken);
                    continue;
                }

                await outboxStore.MarkPublishedAsync(message.Id, cancellationToken);
            }
            catch (Exception ex)
            {
                await outboxStore.MarkFailedAsync(message.Id, ex.Message, cancellationToken);
                _logger.LogError(ex, "Failed to publish outbox message {MessageId}.", message.Id);
            }
        }
    }
}
