namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// Options that control how a message is scheduled for future delivery.
/// </summary>
public sealed class ScheduleOptions
{
    /// <summary>
    /// Gets or sets the destination topic or queue for the scheduled message.
    /// </summary>
    public string? Topic { get; set; }

    /// <summary>
    /// Gets or sets the routing key used by transports that support topic routing.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets or sets the correlation identifier propagated with the message.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the causation identifier that links this message to its trigger.
    /// </summary>
    public string? CausationId { get; set; }

    /// <summary>
    /// Gets or sets transport-specific or application headers attached to the message.
    /// </summary>
    public IDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Gets or sets an optional schedule group identifier for cancellation and management.
    /// </summary>
    public string? ScheduleGroupId { get; set; }

    /// <summary>
    /// Gets or sets an optional user-defined schedule identifier.
    /// When not set, the scheduler assigns one.
    /// </summary>
    public string? ScheduleId { get; set; }

    /// <summary>
    /// Gets or sets the time zone identifier used when <see cref="ScheduledAt"/> is local.
    /// </summary>
    public string? TimeZoneId { get; set; }

    /// <summary>
    /// Gets or sets the absolute UTC time at which the message should be delivered.
    /// </summary>
    public DateTimeOffset? ScheduledAt { get; set; }

    /// <summary>
    /// Gets or sets publish options applied when the scheduled message is eventually published.
    /// </summary>
    public PublishOptions? PublishOptions { get; set; }

    /// <summary>
    /// Creates a shallow copy of the current options instance.
    /// </summary>
    public ScheduleOptions Clone() => new()
    {
        Topic = Topic,
        RoutingKey = RoutingKey,
        CorrelationId = CorrelationId,
        CausationId = CausationId,
        Headers = Headers is null ? null : new Dictionary<string, string>(Headers),
        ScheduleGroupId = ScheduleGroupId,
        ScheduleId = ScheduleId,
        TimeZoneId = TimeZoneId,
        ScheduledAt = ScheduledAt,
        PublishOptions = PublishOptions?.Clone(),
    };
}
