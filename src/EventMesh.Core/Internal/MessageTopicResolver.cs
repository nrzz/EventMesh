using EventMesh.Abstractions.Configuration;
using Microsoft.Extensions.Options;

namespace EventMesh.Core.Internal;

/// <summary>
/// Resolves destination topics and queues from message types and options.
/// </summary>
public sealed class MessageTopicResolver
{
    private readonly EventMeshOptions _options;

    public MessageTopicResolver(IOptions<EventMeshOptions> options)
    {
        _options = options.Value;
    }

    public string ResolveTopic<T>(string? explicitTopic = null)
    {
        var topic = explicitTopic ?? typeof(T).FullName
            ?? throw new InvalidOperationException($"Cannot resolve topic for type '{typeof(T).Name}'.");

        if (!string.IsNullOrWhiteSpace(_options.TopicPrefix))
        {
            topic = $"{_options.TopicPrefix}.{topic}";
        }

        return topic;
    }

    public string ResolveMessageType<T>(string? explicitType = null) =>
        explicitType ?? typeof(T).FullName
        ?? throw new InvalidOperationException($"Cannot resolve message type for '{typeof(T).Name}'.");

    public string ResolveSource(string? explicitSource = null) =>
        explicitSource ?? _options.ApplicationName;
}
