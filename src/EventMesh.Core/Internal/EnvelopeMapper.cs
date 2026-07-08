using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;

namespace EventMesh.Core.Internal;

/// <summary>
/// Maps between CloudEvents envelopes and transport messages.
/// </summary>
internal static class EnvelopeMapper
{
    public static TransportMessage ToTransportMessage(
        MessageEnvelope envelope,
        string destination,
        PublishOptions? options = null)
    {
        var headers = new Dictionary<string, string>(envelope.Headers, StringComparer.OrdinalIgnoreCase);

        headers["ce-specversion"] = envelope.SpecVersion;
        headers["ce-id"] = envelope.Id;
        headers["ce-source"] = envelope.Source;
        headers["ce-type"] = envelope.Type;

        if (!string.IsNullOrWhiteSpace(envelope.Subject))
        {
            headers["ce-subject"] = envelope.Subject;
        }

        if (envelope.Time is not null)
        {
            headers["ce-time"] = envelope.Time.Value.ToString("O");
        }

        if (options?.Headers is not null)
        {
            foreach (var header in options.Headers)
            {
                headers[header.Key] = header.Value;
            }
        }

        return new TransportMessage
        {
            MessageId = envelope.Id,
            Destination = destination,
            RoutingKey = options?.RoutingKey,
            Body = envelope.Data ?? ReadOnlyMemory<byte>.Empty,
            ContentType = envelope.DataContentType,
            Headers = headers,
            CorrelationId = envelope.CorrelationId,
            Priority = options?.Priority,
            TimeToLive = options?.TimeToLive,
            SessionId = options?.SessionId,
            PartitionKey = options?.PartitionKey,
        };
    }

    public static MessageEnvelope FromTransportMessage(TransportMessage message)
    {
        var builder = MessageEnvelope.Create()
            .WithId(message.MessageId ?? Guid.NewGuid().ToString("N"))
            .WithSource(GetHeader(message, "ce-source") ?? "eventmesh/transport")
            .WithType(GetHeader(message, "ce-type") ?? "eventmesh.unknown")
            .WithData(message.Body)
            .WithDataContentType(message.ContentType)
            .WithCorrelationId(message.CorrelationId);

        if (GetHeader(message, "ce-specversion") is { } specVersion)
        {
            builder.WithSpecVersion(specVersion);
        }

        if (GetHeader(message, "ce-subject") is { } subject)
        {
            builder.WithSubject(subject);
        }

        if (GetHeader(message, "ce-causationid") is { } causationId)
        {
            builder.WithCausationId(causationId);
        }

        if (GetHeader(message, "ce-time") is { } time && DateTimeOffset.TryParse(time, out var parsedTime))
        {
            builder.WithTime(parsedTime);
        }

        foreach (var header in message.Headers)
        {
            if (!header.Key.StartsWith("ce-", StringComparison.OrdinalIgnoreCase))
            {
                builder.WithHeader(header.Key, header.Value);
            }
        }

        return builder.Build();
    }

    private static string? GetHeader(TransportMessage message, string name) =>
        message.Headers.TryGetValue(name, out var value) ? value : null;
}
