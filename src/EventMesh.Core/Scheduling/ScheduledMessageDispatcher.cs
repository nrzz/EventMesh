using EventMesh.Abstractions.Scheduling;
using EventMesh.Abstractions.Transport;
using EventMesh.Core.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventMesh.Core.Scheduling;

/// <summary>
/// Background service that dispatches due scheduled messages through the transport.
/// </summary>
public sealed class ScheduledMessageDispatcher : IHostedService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private const int BatchSize = 100;

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly InMemoryMessageScheduler _scheduler;
    private readonly ILogger<ScheduledMessageDispatcher> _logger;
    private CancellationTokenSource? _hostCts;
    private Task? _hostTask;

    public ScheduledMessageDispatcher(
        IServiceScopeFactory scopeFactory,
        InMemoryMessageScheduler scheduler,
        ILogger<ScheduledMessageDispatcher> logger)
    {
        _scopeFactory = scopeFactory;
        _scheduler = scheduler;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _hostCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _hostTask = RunAsync(_hostCts.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
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
                await DispatchDueMessagesAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error dispatching scheduled messages.");
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

    private async Task DispatchDueMessagesAsync(CancellationToken cancellationToken)
    {
        var dueMessages = await _scheduler.GetDueAsync(DateTimeOffset.UtcNow, BatchSize, cancellationToken);
        if (dueMessages.Count == 0)
        {
            return;
        }

        using var scope = _scopeFactory.CreateScope();
        var transport = scope.ServiceProvider.GetRequiredService<IBrokerTransport>();

        foreach (var scheduled in dueMessages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _scheduler.MarkDispatching(scheduled.ScheduleId);

            try
            {
                var transportMessage = EnvelopeMapper.ToTransportMessage(
                    scheduled.Envelope,
                    scheduled.Destination,
                    scheduled.Options?.PublishOptions);

                var result = await transport.SendAsync(transportMessage, cancellationToken);
                if (!result.Succeeded)
                {
                    _scheduler.MarkFailed(scheduled.ScheduleId);
                    _logger.LogWarning(
                        "Failed to dispatch scheduled message {ScheduleId}: {Error}",
                        scheduled.ScheduleId,
                        result.ErrorMessage);
                    continue;
                }

                _scheduler.MarkDelivered(scheduled.ScheduleId);
            }
            catch (Exception ex)
            {
                _scheduler.MarkFailed(scheduled.ScheduleId);
                _logger.LogError(ex, "Failed to dispatch scheduled message {ScheduleId}.", scheduled.ScheduleId);
            }
        }
    }
}
