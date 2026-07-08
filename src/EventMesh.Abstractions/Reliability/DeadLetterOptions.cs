namespace EventMesh.Abstractions.Reliability;

/// <summary>
/// Options that control dead-letter queue behavior for failed messages.
/// </summary>
public sealed class DeadLetterOptions
{
    /// <summary>
    /// Gets or sets the dead-letter destination queue or topic name.
    /// </summary>
    public string? Destination { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of delivery attempts before a message is dead-lettered.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>
    /// Gets or sets a value indicating whether the original envelope metadata should be preserved.
    /// </summary>
    public bool PreserveEnvelope { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether the failure reason should be attached as a header.
    /// </summary>
    public bool IncludeFailureReason { get; set; } = true;

    /// <summary>
    /// Gets or sets the header name used to store the failure reason.
    /// </summary>
    public string FailureReasonHeader { get; set; } = "x-eventmesh-dead-letter-reason";

    /// <summary>
    /// Gets or sets additional headers applied to dead-lettered messages.
    /// </summary>
    public IDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Creates a shallow copy of the current dead-letter options.
    /// </summary>
    public DeadLetterOptions Clone() => new()
    {
        Destination = Destination,
        MaxDeliveryAttempts = MaxDeliveryAttempts,
        PreserveEnvelope = PreserveEnvelope,
        IncludeFailureReason = IncludeFailureReason,
        FailureReasonHeader = FailureReasonHeader,
        Headers = Headers is null ? null : new Dictionary<string, string>(Headers),
    };
}
