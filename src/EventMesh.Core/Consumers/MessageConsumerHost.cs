using EventMesh.Abstractions.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventMesh.Core.Consumers;

/// <summary>
/// Background service that hosts registered message consumers.
/// </summary>
public sealed class MessageConsumerHost : IHostedService
{
    private readonly IConsumerManager _consumerManager;
    private readonly ILogger<MessageConsumerHost> _logger;
    private CancellationTokenSource? _hostCts;
    private Task? _hostTask;

    public MessageConsumerHost(IConsumerManager consumerManager, ILogger<MessageConsumerHost> logger)
    {
        _consumerManager = consumerManager;
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

        foreach (var consumer in _consumerManager.Consumers)
        {
            try
            {
                await consumer.StopAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Error stopping consumer {ConsumerId} on subscription {Subscription}.",
                    consumer.ConsumerId,
                    consumer.Subscription);
            }
        }
    }

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        var consumers = _consumerManager.Consumers;
        if (consumers.Count == 0)
        {
            _logger.LogDebug("No message consumers registered.");
            return;
        }

        foreach (var consumer in consumers)
        {
            if (consumer.IsRunning)
            {
                continue;
            }

            await consumer.StartAsync(stoppingToken);
            _logger.LogInformation(
                "Started consumer {ConsumerId} on subscription {Subscription}.",
                consumer.ConsumerId,
                consumer.Subscription);
        }

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
