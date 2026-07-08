using System.Diagnostics.Metrics;

namespace EventMesh.Core.Observability;

/// <summary>
/// System.Diagnostics.Metrics instrumentation for EventMesh messaging operations.
/// </summary>
public sealed class EventMeshMetrics
{
    private readonly Counter<long> _messagesPublished;
    private readonly Counter<long> _messagesConsumed;
    private readonly Counter<long> _messagesRetried;
    private readonly Counter<long> _messagesDeadLettered;
    private readonly Counter<long> _publishFailures;
    private readonly Counter<long> _consumeFailures;
    private readonly Histogram<double> _publishDuration;
    private readonly Histogram<double> _consumeDuration;

    /// <summary>
    /// Initializes a new instance of the <see cref="EventMeshMetrics"/> class.
    /// </summary>
    public EventMeshMetrics()
    {
        var meter = new Meter(EventMeshActivitySource.Name, "1.0.0");

        _messagesPublished = meter.CreateCounter<long>(
            "eventmesh.messages.published",
            description: "Total messages published through the mesh.");

        _messagesConsumed = meter.CreateCounter<long>(
            "eventmesh.messages.consumed",
            description: "Total messages consumed through the mesh.");

        _messagesRetried = meter.CreateCounter<long>(
            "eventmesh.messages.retried",
            description: "Total message consumption retries.");

        _messagesDeadLettered = meter.CreateCounter<long>(
            "eventmesh.messages.dead_lettered",
            description: "Total messages routed to dead-letter destinations.");

        _publishFailures = meter.CreateCounter<long>(
            "eventmesh.publish.failures",
            description: "Total publish operation failures.");

        _consumeFailures = meter.CreateCounter<long>(
            "eventmesh.consume.failures",
            description: "Total consume operation failures.");

        _publishDuration = meter.CreateHistogram<double>(
            "eventmesh.publish.duration",
            unit: "ms",
            description: "Duration of publish operations in milliseconds.");

        _consumeDuration = meter.CreateHistogram<double>(
            "eventmesh.consume.duration",
            unit: "ms",
            description: "Duration of consume operations in milliseconds.");
    }

    /// <summary>
    /// Records a successful message publish.
    /// </summary>
    public void RecordPublished(string? destination = null, string? messageType = null)
    {
        _messagesPublished.Add(1, CreateTags(destination, messageType));
    }

    /// <summary>
    /// Records a successful message consumption.
    /// </summary>
    public void RecordConsumed(string? destination = null, string? messageType = null)
    {
        _messagesConsumed.Add(1, CreateTags(destination, messageType));
    }

    /// <summary>
    /// Records a message consumption retry.
    /// </summary>
    public void RecordRetried(string? destination = null, string? messageType = null)
    {
        _messagesRetried.Add(1, CreateTags(destination, messageType));
    }

    /// <summary>
    /// Records a message routed to a dead-letter destination.
    /// </summary>
    public void RecordDeadLettered(string? destination = null, string? messageType = null)
    {
        _messagesDeadLettered.Add(1, CreateTags(destination, messageType));
    }

    /// <summary>
    /// Records a publish failure.
    /// </summary>
    public void RecordPublishFailure(string? destination = null, string? messageType = null)
    {
        _publishFailures.Add(1, CreateTags(destination, messageType));
    }

    /// <summary>
    /// Records a consume failure.
    /// </summary>
    public void RecordConsumeFailure(string? destination = null, string? messageType = null)
    {
        _consumeFailures.Add(1, CreateTags(destination, messageType));
    }

    /// <summary>
    /// Records the duration of a publish operation.
    /// </summary>
    public void RecordPublishDuration(double durationMs, string? destination = null, string? messageType = null)
    {
        _publishDuration.Record(durationMs, CreateTags(destination, messageType));
    }

    /// <summary>
    /// Records the duration of a consume operation.
    /// </summary>
    public void RecordConsumeDuration(double durationMs, string? destination = null, string? messageType = null)
    {
        _consumeDuration.Record(durationMs, CreateTags(destination, messageType));
    }

    private static KeyValuePair<string, object?>[] CreateTags(string? destination, string? messageType)
    {
        return
        [
            new("eventmesh.destination", destination),
            new("eventmesh.message_type", messageType),
        ];
    }
}
