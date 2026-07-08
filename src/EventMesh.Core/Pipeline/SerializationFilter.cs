using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Observability;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Serialization;
using EventMesh.Core.Internal;
using EventMesh.Core.Observability;
using Microsoft.Extensions.Options;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Serializes and deserializes message payloads through the filter pipeline.
/// </summary>
public sealed class SerializationFilter<T> : IPublishFilter<T>, IConsumeFilter<T> where T : notnull
{
    private readonly IMessageSerializer _serializer;
    private readonly MessageTopicResolver _topicResolver;
    private readonly ICorrelationContext _correlationContext;
    private readonly EventMeshOptions _options;

    public SerializationFilter(
        IMessageSerializer serializer,
        MessageTopicResolver topicResolver,
        ICorrelationContext correlationContext,
        IOptions<EventMeshOptions> options)
    {
        _serializer = serializer;
        _topicResolver = topicResolver;
        _correlationContext = correlationContext;
        _options = options.Value;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        var contentType = context.Options.ContentType ?? _serializer.DefaultContentType ?? _options.DefaultContentType;
        var data = await _serializer.SerializeAsync(context.Message, contentType, cancellationToken);

        var builder = MessageEnvelope.Create()
            .WithSource(_topicResolver.ResolveSource(context.Options.Source))
            .WithType(_topicResolver.ResolveMessageType<T>(context.Options.MessageType))
            .WithData(data)
            .WithDataContentType(contentType)
            .WithSubject(context.Options.Subject)
            .WithCorrelationId(context.Options.CorrelationId ?? _correlationContext.CorrelationId)
            .WithCausationId(context.Options.CausationId ?? _correlationContext.CausationId);

        foreach (var header in _options.GlobalHeaders)
        {
            builder.WithHeader(header.Key, header.Value);
        }

        if (context.Options.Headers is not null)
        {
            builder.WithHeaders(context.Options.Headers);
        }

        context.Envelope = builder.Build();
        context.Destination ??= _topicResolver.ResolveTopic<T>(context.Options.Topic);
        await next(context, cancellationToken);
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Envelope.Data is { Length: > 0 })
        {
            var contentType = context.Envelope.DataContentType ?? _serializer.DefaultContentType ?? _options.DefaultContentType;
            context.Message = await _serializer.DeserializeAsync<T>(context.Envelope.Data.Value, contentType, cancellationToken);
        }

        await next(context, cancellationToken);
    }
}
