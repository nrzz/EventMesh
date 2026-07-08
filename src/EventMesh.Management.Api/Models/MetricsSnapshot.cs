namespace EventMesh.Management.Api.Models;

/// <summary>
/// Snapshot of EventMesh operational metrics.
/// </summary>
public sealed class MetricsSnapshot
{
    /// <summary>
    /// Gets or sets when the snapshot was captured.
    /// </summary>
    public DateTimeOffset CapturedAt { get; init; }

    /// <summary>
    /// Gets or sets counter and gauge metric values.
    /// </summary>
    public IReadOnlyList<MetricValue> Metrics { get; init; } = [];
}

/// <summary>
/// A single metric data point.
/// </summary>
public sealed class MetricValue
{
    /// <summary>
    /// Gets or sets the metric name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the metric type (counter, gauge, histogram).
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Gets or sets the metric value.
    /// </summary>
    public double Value { get; init; }

    /// <summary>
    /// Gets or sets metric dimensions.
    /// </summary>
    public IReadOnlyDictionary<string, string> Tags { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets the metric unit.
    /// </summary>
    public string? Unit { get; init; }

    /// <summary>
    /// Gets or sets the metric description.
    /// </summary>
    public string? Description { get; init; }
}
