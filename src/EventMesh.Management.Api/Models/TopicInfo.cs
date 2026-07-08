namespace EventMesh.Management.Api.Models;

/// <summary>
/// Describes a messaging topic or exchange.
/// </summary>
public sealed class TopicInfo
{
    /// <summary>
    /// Gets or sets the topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the transport hosting the topic.
    /// </summary>
    public required string Transport { get; init; }

    /// <summary>
    /// Gets or sets the topic type (topic, exchange, stream).
    /// </summary>
    public string Type { get; init; } = "topic";

    /// <summary>
    /// Gets or sets the number of partitions or shards.
    /// </summary>
    public int Partitions { get; init; } = 1;

    /// <summary>
    /// Gets or sets the approximate message count.
    /// </summary>
    public long MessageCount { get; init; }

    /// <summary>
    /// Gets or sets the publish rate per second.
    /// </summary>
    public double PublishRate { get; init; }

    /// <summary>
    /// Gets or sets when the topic was created or first observed.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; }
}

/// <summary>
/// Request to create or update a topic.
/// </summary>
public sealed class CreateTopicRequest
{
    /// <summary>
    /// Gets or sets the topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets or sets the target transport.
    /// </summary>
    public string? Transport { get; init; }

    /// <summary>
    /// Gets or sets the number of partitions.
    /// </summary>
    public int Partitions { get; init; } = 1;
}
