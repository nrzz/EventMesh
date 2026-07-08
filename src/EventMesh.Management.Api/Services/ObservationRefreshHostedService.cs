using EventMesh.Management.Api.Services;
using Microsoft.Extensions.Options;
using EventMesh.Management.Api.Configuration;

namespace EventMesh.Management.Api.Services;

/// <summary>
/// Periodically refreshes mesh observation state and broadcasts updates.
/// </summary>
public sealed class ObservationRefreshHostedService : BackgroundService
{
    private readonly IMeshObservationService _observationService;
    private readonly ManagementApiOptions _options;
    private readonly ILogger<ObservationRefreshHostedService> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="ObservationRefreshHostedService"/> class.
    /// </summary>
    public ObservationRefreshHostedService(
        IMeshObservationService observationService,
        IOptions<ManagementApiOptions> options,
        ILogger<ObservationRefreshHostedService> logger)
    {
        _observationService = observationService;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(1, _options.RefreshIntervalSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _observationService.RefreshAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to refresh mesh observation state.");
            }

            try
            {
                await Task.Delay(interval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }
}
