using System.Text.Json;
using System.Text.Json.Serialization;
using EventMesh.Abstractions.Envelope;

namespace EventMesh.Storage.PostgreSql.Internal;

internal static class MessageEnvelopeJson
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string Serialize(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var dto = EnvelopeDto.FromEnvelope(envelope);
        return JsonSerializer.Serialize(dto, SerializerOptions);
    }

    public static MessageEnvelope Deserialize(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);

        var dto = JsonSerializer.Deserialize<EnvelopeDto>(json, SerializerOptions)
            ?? throw new InvalidOperationException("Envelope JSON deserialized to null.");

        return dto.ToEnvelope();
    }

    public static string SerializeScheduledPayload(ScheduledMessagePayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        return JsonSerializer.Serialize(payload, SerializerOptions);
    }

    public static ScheduledMessagePayload DeserializeScheduledPayload(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);

        return JsonSerializer.Deserialize<ScheduledMessagePayload>(json, SerializerOptions)
            ?? throw new InvalidOperationException("Scheduled message JSON deserialized to null.");
    }

    internal sealed class EnvelopeDto
    {
        public string SpecVersion { get; set; } = "1.0";

        public required string Id { get; set; }

        public required string Source { get; set; }

        public required string Type { get; set; }

        public string? DataContentType { get; set; }

        public string? DataBase64 { get; set; }

        public DateTimeOffset? Time { get; set; }

        public string? Subject { get; set; }

        public string? CorrelationId { get; set; }

        public string? CausationId { get; set; }

        public Dictionary<string, string>? Headers { get; set; }

        public static EnvelopeDto FromEnvelope(MessageEnvelope envelope)
        {
            return new EnvelopeDto
            {
                SpecVersion = envelope.SpecVersion,
                Id = envelope.Id,
                Source = envelope.Source,
                Type = envelope.Type,
                DataContentType = envelope.DataContentType,
                DataBase64 = envelope.Data is { Length: > 0 } data
                    ? Convert.ToBase64String(data.Span)
                    : null,
                Time = envelope.Time,
                Subject = envelope.Subject,
                CorrelationId = envelope.CorrelationId,
                CausationId = envelope.CausationId,
                Headers = envelope.Headers.Count > 0
                    ? new Dictionary<string, string>(envelope.Headers, StringComparer.OrdinalIgnoreCase)
                    : null,
            };
        }

        public MessageEnvelope ToEnvelope()
        {
            ReadOnlyMemory<byte>? data = null;
            if (!string.IsNullOrEmpty(DataBase64))
            {
                data = Convert.FromBase64String(DataBase64);
            }

            return new MessageEnvelope
            {
                SpecVersion = SpecVersion,
                Id = Id,
                Source = Source,
                Type = Type,
                DataContentType = DataContentType,
                Data = data,
                Time = Time,
                Subject = Subject,
                CorrelationId = CorrelationId,
                CausationId = CausationId,
                Headers = Headers is { Count: > 0 }
                    ? new Dictionary<string, string>(Headers, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            };
        }
    }

    internal sealed class ScheduledMessagePayload
    {
        public required MessageEnvelope Envelope { get; set; }

        public string? ScheduleGroupId { get; set; }

        public Dictionary<string, string>? Options { get; set; }
    }
}
