using System.Diagnostics;

namespace EventMesh.Core.Observability;

/// <summary>
/// OpenTelemetry activity source for EventMesh messaging operations.
/// </summary>
public static class EventMeshActivitySource
{
    /// <summary>
    /// The activity source name used for EventMesh instrumentation.
    /// </summary>
    public const string Name = "EventMesh";

    /// <summary>
    /// The shared activity source instance.
    /// </summary>
    public static readonly ActivitySource Instance = new(Name, "1.0.0");
}
