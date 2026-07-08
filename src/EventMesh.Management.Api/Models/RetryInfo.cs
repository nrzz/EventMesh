namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes a message retry attempt.
/// </summary>
public sealed class RetryInfo
{
    /// <summary>
    /// Gets or sets the retry record identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets or sets the original message identifier.
    /// </summary>
    public required string MessageId { get; init; }

    /// <summary>
    /// Gets or sets the destination being retried.
    /// </summary>
    public required string Destination { get; init; }

    /// <summary>
    /// Gets or sets the transport name.
    /// </summary>
    public required string Transport { get; init; }

    /// <summary>
    /// Gets or sets the current retry attempt number.
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>
    /// Gets or sets the maximum retry attempts.
    /// </summary>
    public int MaxAttempts { get; init; }

    /// <summary>
    /// Gets or sets when the next retry is scheduled.
    /// </summary>
    public DateTimeOffset? NextRetryAt { get; init; }

    /// <summary>
    /// Gets or sets the failure reason.
    /// </summary>
    public string? FailureReason { get; init; }

    /// <summary>
    /// Gets or sets when the retry was first recorded.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; }
}
