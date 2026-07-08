using System.Diagnostics;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Core.Observability;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Records publish and consume metrics through <see cref="EventMeshMetrics"/>.
/// </summary>
public sealed class MetricsFilter<T> : IPublishFilter<T>, IConsumeFilter<T> where T : notnull
{
    private readonly EventMeshMetrics _metrics;

    public MetricsFilter(EventMeshMetrics metrics)
    {
        _metrics = metrics;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            await next(context, cancellationToken);
            _metrics.RecordPublished(context.Destination, context.Envelope?.Type);
        }
        catch
        {
            _metrics.RecordPublishFailure(context.Destination, context.Envelope?.Type);
            throw;
        }
        finally
        {
            stopwatch.Stop();
            _metrics.RecordPublishDuration(stopwatch.Elapsed.TotalMilliseconds, context.Destination, context.Envelope?.Type);
        }
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        var destination = context.Options.Topic ?? context.Envelope.Type;

        try
        {
            await next(context, cancellationToken);
            _metrics.RecordConsumed(destination, context.Envelope.Type);
        }
        catch
        {
            _metrics.RecordConsumeFailure(destination, context.Envelope.Type);
            throw;
        }
        finally
        {
            stopwatch.Stop();
            _metrics.RecordConsumeDuration(stopwatch.Elapsed.TotalMilliseconds, destination, context.Envelope.Type);
        }
    }
}
