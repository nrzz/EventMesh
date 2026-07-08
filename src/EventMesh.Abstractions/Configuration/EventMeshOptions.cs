using EventMesh.Abstractions.Reliability;
using EventMesh.Abstractions.Serialization;

namespace EventMesh.Abstractions.Configuration;

/// <summary>
/// Root configuration options for EventMesh.
/// </summary>
public sealed class EventMeshOptions
{
    /// <summary>
    /// Gets or sets the application name used as the default CloudEvents source.
    /// </summary>
    public string ApplicationName { get; set; } = "eventmesh";

    /// <summary>
    /// Gets or sets the default transport name to use when not specified explicitly.
    /// </summary>
    public string? DefaultTransport { get; set; }

    /// <summary>
    /// Gets or sets the default serialization format for message payloads.
    /// </summary>
    public SerializationFormat DefaultSerializationFormat { get; set; } = SerializationFormat.Json;

    /// <summary>
    /// Gets or sets the default content type for serialized messages.
    /// </summary>
    public string DefaultContentType { get; set; } = "application/json";

    /// <summary>
    /// Gets or sets the default topic prefix applied to message destinations.
    /// </summary>
    public string? TopicPrefix { get; set; }

    /// <summary>
    /// Gets or sets the default request timeout.
    /// </summary>
    public TimeSpan DefaultRequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the default retry policy applied to consumers.
    /// </summary>
    public RetryPolicy DefaultRetryPolicy { get; set; } = new();

    /// <summary>
    /// Gets or sets the default dead-letter options applied to consumers.
    /// </summary>
    public DeadLetterOptions? DefaultDeadLetterOptions { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether outbox dispatch is enabled.
    /// </summary>
    public bool EnableOutbox { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether inbox deduplication is enabled.
    /// </summary>
    public bool EnableInbox { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether OpenTelemetry instrumentation is enabled.
    /// </summary>
    public bool EnableOpenTelemetry { get; set; } = true;

    /// <summary>
    /// Gets or sets transport-specific connection settings keyed by transport name.
    /// </summary>
    public IDictionary<string, IDictionary<string, string>> TransportSettings { get; set; } =
        new Dictionary<string, IDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets plugin-specific configuration values keyed by plugin name.
    /// </summary>
    public IDictionary<string, IDictionary<string, string>> PluginSettings { get; set; } =
        new Dictionary<string, IDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets global headers applied to all published messages.
    /// </summary>
    public IDictionary<string, string> GlobalHeaders { get; set; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets the assembly names scanned for message handlers and sagas.
    /// </summary>
    public IList<string> HandlerAssemblies { get; set; } = new List<string>();

    /// <summary>
    /// Creates a shallow copy of the current options.
    /// </summary>
    public EventMeshOptions Clone() => new()
    {
        ApplicationName = ApplicationName,
        DefaultTransport = DefaultTransport,
        DefaultSerializationFormat = DefaultSerializationFormat,
        DefaultContentType = DefaultContentType,
        TopicPrefix = TopicPrefix,
        DefaultRequestTimeout = DefaultRequestTimeout,
        DefaultRetryPolicy = DefaultRetryPolicy.Clone(),
        DefaultDeadLetterOptions = DefaultDeadLetterOptions?.Clone(),
        EnableOutbox = EnableOutbox,
        EnableInbox = EnableInbox,
        EnableOpenTelemetry = EnableOpenTelemetry,
        TransportSettings = TransportSettings.ToDictionary(
            pair => pair.Key,
            pair => (IDictionary<string, string>)new Dictionary<string, string>(pair.Value),
            StringComparer.OrdinalIgnoreCase),
        PluginSettings = PluginSettings.ToDictionary(
            pair => pair.Key,
            pair => (IDictionary<string, string>)new Dictionary<string, string>(pair.Value),
            StringComparer.OrdinalIgnoreCase),
        GlobalHeaders = new Dictionary<string, string>(GlobalHeaders, StringComparer.OrdinalIgnoreCase),
        HandlerAssemblies = new List<string>(HandlerAssemblies),
    };
}
