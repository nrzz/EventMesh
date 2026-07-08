using System.Globalization;
using System.Text;
using System.Text.Json;
using EventMesh.Abstractions.Transport;
using StackExchange.Redis;

namespace EventMesh.Transport.RedisStreams;

internal static class RedisStreamsMessageFields
{
    public const string MessageId = "em_messageId";
    public const string Body = "em_body";
    public const string ContentType = "em_contentType";
    public const string CorrelationId = "em_correlationId";
    public const string ReplyTo = "em_replyTo";
    public const string RoutingKey = "em_routingKey";
    public const string Headers = "em_headers";
    public const string Priority = "em_priority";
    public const string ScheduledAt = "em_scheduledAt";
    public const string DeliveryCount = "em_deliveryCount";
    public const string EnqueuedAt = "em_enqueuedAt";
    public const string TimeToLive = "em_timeToLive";
    public const string SessionId = "em_sessionId";
    public const string PartitionKey = "em_partitionKey";
}

internal static class RedisStreamsMessageCodec
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static NameValueEntry[] ToStreamEntries(TransportMessage message)
    {
        var messageId = message.MessageId ?? Guid.NewGuid().ToString("N");
        message.MessageId = messageId;
        message.EnqueuedAt ??= DateTimeOffset.UtcNow;

        var entries = new List<NameValueEntry>
        {
            new(RedisStreamsMessageFields.MessageId, messageId),
            new(RedisStreamsMessageFields.Body, Convert.ToBase64String(message.Body.Span)),
            new(RedisStreamsMessageFields.DeliveryCount, message.DeliveryCount.ToString(CultureInfo.InvariantCulture)),
            new(RedisStreamsMessageFields.EnqueuedAt, message.EnqueuedAt.Value.ToString("O", CultureInfo.InvariantCulture)),
        };

        AddOptional(entries, RedisStreamsMessageFields.ContentType, message.ContentType);
        AddOptional(entries, RedisStreamsMessageFields.CorrelationId, message.CorrelationId);
        AddOptional(entries, RedisStreamsMessageFields.ReplyTo, message.ReplyTo);
        AddOptional(entries, RedisStreamsMessageFields.RoutingKey, message.RoutingKey);
        AddOptional(entries, RedisStreamsMessageFields.SessionId, message.SessionId);
        AddOptional(entries, RedisStreamsMessageFields.PartitionKey, message.PartitionKey);

        if (message.Priority is not null)
        {
            entries.Add(new NameValueEntry(
                RedisStreamsMessageFields.Priority,
                message.Priority.Value.ToString(CultureInfo.InvariantCulture)));
        }

        if (message.ScheduledAt is not null)
        {
            entries.Add(new NameValueEntry(
                RedisStreamsMessageFields.ScheduledAt,
                message.ScheduledAt.Value.ToString("O", CultureInfo.InvariantCulture)));
        }

        if (message.TimeToLive is not null)
        {
            entries.Add(new NameValueEntry(
                RedisStreamsMessageFields.TimeToLive,
                message.TimeToLive.Value.ToString("c", CultureInfo.InvariantCulture)));
        }

        if (message.Headers.Count > 0)
        {
            entries.Add(new NameValueEntry(
                RedisStreamsMessageFields.Headers,
                JsonSerializer.Serialize(message.Headers, JsonOptions)));
        }

        return entries.ToArray();
    }

    public static TransportMessage FromStreamEntry(StreamEntry entry)
    {
        var values = entry.Values.ToDictionary(
            static value => value.Name.ToString(),
            static value => value.Value.ToString(),
            StringComparer.Ordinal);

        var body = values.TryGetValue(RedisStreamsMessageFields.Body, out var encodedBody)
            ? Convert.FromBase64String(encodedBody)
            : Array.Empty<byte>();

        var headers = values.TryGetValue(RedisStreamsMessageFields.Headers, out var headersJson)
            ? JsonSerializer.Deserialize<Dictionary<string, string>>(headersJson, JsonOptions)
                ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        return new TransportMessage
        {
            MessageId = GetOptional(values, RedisStreamsMessageFields.MessageId),
            Destination = string.Empty,
            RoutingKey = GetOptional(values, RedisStreamsMessageFields.RoutingKey),
            Body = body,
            ContentType = GetOptional(values, RedisStreamsMessageFields.ContentType),
            Headers = headers,
            CorrelationId = GetOptional(values, RedisStreamsMessageFields.CorrelationId),
            ReplyTo = GetOptional(values, RedisStreamsMessageFields.ReplyTo),
            Priority = GetOptionalInt(values, RedisStreamsMessageFields.Priority),
            TimeToLive = GetOptionalTimeSpan(values, RedisStreamsMessageFields.TimeToLive),
            ScheduledAt = GetOptionalDateTimeOffset(values, RedisStreamsMessageFields.ScheduledAt),
            SessionId = GetOptional(values, RedisStreamsMessageFields.SessionId),
            PartitionKey = GetOptional(values, RedisStreamsMessageFields.PartitionKey),
            DeliveryCount = GetOptionalInt(values, RedisStreamsMessageFields.DeliveryCount) ?? 0,
            EnqueuedAt = GetOptionalDateTimeOffset(values, RedisStreamsMessageFields.EnqueuedAt),
        };
    }

    public static string EncodeDeliveryTag(
        RedisKey streamKey,
        string consumerGroup,
        RedisValue messageId,
        string consumerName) =>
        string.Join('|', streamKey.ToString(), consumerGroup, messageId.ToString(), consumerName);

    public static bool TryDecodeDeliveryTag(
        string deliveryTag,
        out RedisKey streamKey,
        out string consumerGroup,
        out RedisValue messageId,
        out string consumerName)
    {
        streamKey = default;
        consumerGroup = string.Empty;
        messageId = RedisValue.Null;
        consumerName = string.Empty;

        if (string.IsNullOrWhiteSpace(deliveryTag))
        {
            return false;
        }

        var parts = deliveryTag.Split('|', 4);
        if (parts.Length != 4)
        {
            return false;
        }

        streamKey = parts[0];
        consumerGroup = parts[1];
        messageId = parts[2];
        consumerName = parts[3];
        return true;
    }

    public static string EncodeDelayedMember(RedisKey streamKey, TransportMessage message) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(
            $"{streamKey}|{message.MessageId}|{Guid.NewGuid():N}"));

    public static bool TryDecodeDelayedMember(string member, out RedisKey streamKey, out string messageId)
    {
        streamKey = default;
        messageId = string.Empty;

        try
        {
            var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(member));
            var separatorIndex = decoded.IndexOf('|', StringComparison.Ordinal);
            if (separatorIndex <= 0)
            {
                return false;
            }

            streamKey = decoded[..separatorIndex];
            var remainder = decoded[(separatorIndex + 1)..];
            var secondSeparator = remainder.IndexOf('|', StringComparison.Ordinal);
            messageId = secondSeparator > 0 ? remainder[..secondSeparator] : remainder;
            return !string.IsNullOrWhiteSpace(messageId);
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static void AddOptional(List<NameValueEntry> entries, string name, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            entries.Add(new NameValueEntry(name, value));
        }
    }

    private static string? GetOptional(IReadOnlyDictionary<string, string> values, string name) =>
        values.TryGetValue(name, out var value) ? value : null;

    private static int? GetOptionalInt(IReadOnlyDictionary<string, string> values, string name) =>
        values.TryGetValue(name, out var value) && int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;

    private static DateTimeOffset? GetOptionalDateTimeOffset(IReadOnlyDictionary<string, string> values, string name) =>
        values.TryGetValue(name, out var value)
        && DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : null;

    private static TimeSpan? GetOptionalTimeSpan(IReadOnlyDictionary<string, string> values, string name) =>
        values.TryGetValue(name, out var value)
        && TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
}
