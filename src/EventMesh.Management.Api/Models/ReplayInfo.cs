namespace EventMesh.Management.Api.Models;

/// <summary>
/// Request to start a message replay job.
/// </summary>
public sealed class ReplayRequest
{
    /// <summary>
    /// Gets or sets the source topic, stream, or archive.
    /// </summary>
    public required string Source { get; init; }

    /// <summary>
    /// Gets or sets the destination for replayed messages.
    /// </summary>
    public string? Destination { get; init; }

    /// <summary>
    /// Gets or sets the inclusive start time.
    /// </summary>
    public DateTimeOffset? From { get; init; }

    /// <summary>
    /// Gets or sets the exclusive end time.
    /// </summary>
    public DateTimeOffset? To { get; init; }

    /// <summary>
    /// Gets or sets the maximum number of messages to replay.
    /// </summary>
    public int? MaxMessages { get; init; }

    /// <summary>
    /// Gets or sets a filter expression.
    /// </summary>
    public string? Filter { get; init; }
}

/// <summary>
/// Describes a replay job and its progress.
/// </summary>
public sealed class ReplayJobInfo
{
    /// <summary>
    /// Gets or sets the replay job identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets or sets the replay source.
    /// </summary>
    public required string Source { get; init; }

    /// <summary>
    /// Gets or sets the replay destination.
    /// </summary>
    public string? Destination { get; init; }

    /// <summary>
    /// Gets or sets the job status.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Gets or sets messages replayed so far.
    /// </summary>
    public long MessagesReplayed { get; init; }

    /// <summary>
    /// Gets or sets the total messages to replay.
    /// </summary>
    public long? TotalMessages { get; init; }

    /// <summary>
    /// Gets or sets when the job was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    /// Gets or sets when the job completed.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    /// Gets or sets an error message if the job failed.
    /// </summary>
    public string? Error { get; init; }
}
