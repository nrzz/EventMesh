namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes a broker or infrastructure connection monitored by the control plane.
/// </summary>
public sealed class ConnectionInfo
{
    /// <summary>
    /// Gets or sets the connection identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets or sets the transport or service name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the connection type (transport, database, cache).
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Gets or sets the connection endpoint.
    /// </summary>
    public string? Endpoint { get; init; }

    /// <summary>
    /// Gets or sets the connection health status.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Gets or sets the measured round-trip latency in milliseconds.
    /// </summary>
    public double? LatencyMs { get; init; }

    /// <summary>
    /// Gets or sets when the connection was last checked.
    /// </summary>
    public DateTimeOffset LastCheckedAt { get; init; }

    /// <summary>
    /// Gets or sets optional error details when unhealthy.
    /// </summary>
    public string? Error { get; init; }
}
