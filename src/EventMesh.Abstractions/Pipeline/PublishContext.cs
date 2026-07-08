using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Messaging;

namespace EventMesh.Abstractions.Pipeline;

/// <summary>
/// Context for a message being published through the filter pipeline.
/// </summary>
/// <typeparam name="T">The message payload type.</typeparam>
public sealed class PublishContext<T> : FilterContext where T : notnull
{
    /// <summary>
    /// Gets or sets the message payload being published.
    /// </summary>
    public required T Message { get; set; }

    /// <summary>
    /// Gets or sets the publish options for the operation.
    /// </summary>
    public PublishOptions Options { get; set; } = new();

    /// <summary>
    /// Gets or sets the CloudEvents envelope constructed for the message.
    /// </summary>
    public MessageEnvelope? Envelope { get; set; }

    /// <summary>
    /// Gets or sets the destination topic or queue resolved for the publish operation.
    /// </summary>
    public string? Destination { get; set; }
}
