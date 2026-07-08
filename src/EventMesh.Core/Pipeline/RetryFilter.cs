using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Reliability;
using EventMesh.Core.Observability;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Retries failed message consumption using a configured <see cref="RetryPolicy"/>.
/// </summary>
public sealed class RetryFilter<T> : IConsumeFilter<T> where T : notnull
{
    private readonly EventMeshOptions _meshOptions;
    private readonly EventMeshMetrics _metrics;
    private readonly ILogger<RetryFilter<T>> _logger;

    public RetryFilter(
        IOptions<EventMeshOptions> meshOptions,
        EventMeshMetrics metrics,
        ILogger<RetryFilter<T>> logger)
    {
        _meshOptions = meshOptions.Value;
        _metrics = metrics;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        var policy = context.Options.RetryPolicy?.Clone() ?? _meshOptions.DefaultRetryPolicy.Clone();
        var attempt = 0;

        while (true)
        {
            try
            {
                await next(context, cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt < policy.MaxRetries && policy.IsRetryable(ex))
            {
                attempt++;
                context.DeliveryCount++;
                _metrics.RecordRetried(context.Options.Topic, context.Envelope.Type);
                _logger.LogWarning(
                    ex,
                    "Retrying message consumption attempt {Attempt}/{MaxRetries} for type {MessageType}.",
                    attempt,
                    policy.MaxRetries,
                    context.Envelope.Type);

                var delay = policy.CalculateDelay(attempt - 1);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken);
                }
            }
        }
    }
}
