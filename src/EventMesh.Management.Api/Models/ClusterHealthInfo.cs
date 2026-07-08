namespace EventMesh.Management.Api.Models;

/// <summary>
/// Aggregated cluster health information.
/// </summary>
public sealed class ClusterHealthInfo
{
    /// <summary>
    /// Gets or sets the overall cluster status.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Gets or sets the management API version.
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Gets or sets when health was evaluated.
    /// </summary>
    public DateTimeOffset CheckedAt { get; init; }

    /// <summary>
    /// Gets or sets individual component health checks.
    /// </summary>
    public IReadOnlyList<ComponentHealth> Components { get; init; } = [];
}

/// <summary>
/// Health status for an individual cluster component.
/// </summary>
public sealed class ComponentHealth
{
    /// <summary>
    /// Gets or sets the component name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the component status.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Gets or sets optional status description.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Gets or sets component-specific metadata.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}

/// <summary>
/// Dashboard overview summary.
/// </summary>
public sealed class OverviewInfo
{
    /// <summary>
    /// Gets or sets the cluster health status.
    /// </summary>
    public required string ClusterStatus { get; init; }

    /// <summary>
    /// Gets or sets the total connection count.
    /// </summary>
    public int ConnectionCount { get; init; }

    /// <summary>
    /// Gets or sets the number of healthy connections.
    /// </summary>
    public int HealthyConnections { get; init; }

    /// <summary>
    /// Gets or sets the total topic count.
    /// </summary>
    public int TopicCount { get; init; }

    /// <summary>
    /// Gets or sets the total queue count.
    /// </summary>
    public int QueueCount { get; init; }

    /// <summary>
    /// Gets or sets the total queue depth across all queues.
    /// </summary>
    public long TotalQueueDepth { get; init; }

    /// <summary>
    /// Gets or sets the active consumer count.
    /// </summary>
    public int ActiveConsumers { get; init; }

    /// <summary>
    /// Gets or sets pending retry count.
    /// </summary>
    public int PendingRetries { get; init; }

    /// <summary>
    /// Gets or sets dead-letter message count.
    /// </summary>
    public int DeadLetterCount { get; init; }

    /// <summary>
    /// Gets or sets messages published per second.
    /// </summary>
    public double MessagesPublishedPerSecond { get; init; }

    /// <summary>
    /// Gets or sets messages consumed per second.
    /// </summary>
    public double MessagesConsumedPerSecond { get; init; }

    /// <summary>
    /// Gets or sets when the overview was generated.
    /// </summary>
    public DateTimeOffset GeneratedAt { get; init; }
}
