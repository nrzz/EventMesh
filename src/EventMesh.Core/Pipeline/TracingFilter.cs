using System.Diagnostics;
using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Core.Observability;
using Microsoft.Extensions.Options;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Creates and propagates OpenTelemetry spans for publish and consume operations.
/// </summary>
public sealed class TracingFilter<T> : IPublishFilter<T>, IConsumeFilter<T> where T : notnull
{
    private readonly EventMeshOptions _options;

    public TracingFilter(IOptions<EventMeshOptions> options)
    {
        _options = options.Value;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (!_options.EnableOpenTelemetry)
        {
            await next(context, cancellationToken);
            return;
        }

        using var activity = EventMeshActivitySource.Instance.StartActivity(
            "eventmesh.publish",
            ActivityKind.Producer);

        activity?.SetTag("eventmesh.message_type", typeof(T).FullName);
        activity?.SetTag("messaging.system", "eventmesh");
        activity?.SetTag("messaging.destination", context.Destination ?? context.Options.Topic);

        if (context.Envelope?.CorrelationId is { } correlationId)
        {
            activity?.SetTag("eventmesh.correlation_id", correlationId);
        }

        try
        {
            await next(context, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (!_options.EnableOpenTelemetry)
        {
            await next(context, cancellationToken);
            return;
        }

        using var activity = EventMeshActivitySource.Instance.StartActivity(
            "eventmesh.consume",
            ActivityKind.Consumer);

        activity?.SetTag("eventmesh.message_type", typeof(T).FullName);
        activity?.SetTag("messaging.system", "eventmesh");
        activity?.SetTag("messaging.destination", context.Options.Topic ?? context.Envelope.Type);

        if (context.Envelope.CorrelationId is { } correlationId)
        {
            activity?.SetTag("eventmesh.correlation_id", correlationId);
        }

        try
        {
            await next(context, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
}
