using System.Globalization;
using System.Text;
using Confluent.Kafka;
using EventMesh.Abstractions.Transport;

namespace EventMesh.Transport.Kafka;

internal static class KafkaMessageCodec
{
    internal const string MessageIdHeader = "x-eventmesh-message-id";
    internal const string CorrelationIdHeader = "x-eventmesh-correlation-id";
    internal const string ReplyToHeader = "x-eventmesh-reply-to";
    internal const string RoutingKeyHeader = "x-eventmesh-routing-key";
    internal const string ContentTypeHeader = "content-type";
    internal const string DeliveryCountHeader = "x-eventmesh-delivery-count";
    internal const string EnqueuedAtHeader = "x-eventmesh-enqueued-at";
    internal const string ScheduledAtHeader = "x-eventmesh-scheduled-at";
    internal const string SessionIdHeader = "x-eventmesh-session-id";
    internal const string PartitionKeyHeader = "x-eventmesh-partition-key";
    internal const string DeadLetterReasonHeader = "x-eventmesh-dead-letter-reason";
    internal const string ReplayHeader = "x-eventmesh-replay";

    internal static Headers ToKafkaHeaders(TransportMessage message)
    {
        var headers = new Headers();

        AddHeader(headers, MessageIdHeader, message.MessageId);
        AddHeader(headers, CorrelationIdHeader, message.CorrelationId);
        AddHeader(headers, ReplyToHeader, message.ReplyTo);
        AddHeader(headers, RoutingKeyHeader, message.RoutingKey);
        AddHeader(headers, ContentTypeHeader, message.ContentType);
        AddHeader(headers, SessionIdHeader, message.SessionId);
        AddHeader(headers, PartitionKeyHeader, message.PartitionKey ?? message.SessionId);
        AddHeader(headers, DeliveryCountHeader, message.DeliveryCount.ToString(CultureInfo.InvariantCulture));

        if (message.EnqueuedAt is not null)
        {
            AddHeader(headers, EnqueuedAtHeader, message.EnqueuedAt.Value.ToString("O", CultureInfo.InvariantCulture));
        }

        if (message.ScheduledAt is not null)
        {
            AddHeader(headers, ScheduledAtHeader, message.ScheduledAt.Value.ToString("O", CultureInfo.InvariantCulture));
        }

        foreach (var header in message.Headers)
        {
            if (IsReservedHeader(header.Key))
            {
                continue;
            }

            AddHeader(headers, header.Key, header.Value);
        }

        return headers;
    }

    internal static TransportMessage FromKafkaRecord(string destination, byte[] body, Headers? headers)
    {
        var message = new TransportMessage
        {
            Destination = destination,
            Body = body,
            Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
        };

        if (headers is null)
        {
            return message;
        }

        foreach (var header in headers)
        {
            var value = Encoding.UTF8.GetString(header.GetValueBytes());
            switch (header.Key)
            {
                case MessageIdHeader:
                    message.MessageId = value;
                    break;
                case CorrelationIdHeader:
                    message.CorrelationId = value;
                    break;
                case ReplyToHeader:
                    message.ReplyTo = value;
                    break;
                case RoutingKeyHeader:
                    message.RoutingKey = value;
                    break;
                case ContentTypeHeader:
                    message.ContentType = value;
                    break;
                case DeliveryCountHeader when int.TryParse(value, out var deliveryCount):
                    message.DeliveryCount = deliveryCount;
                    break;
                case EnqueuedAtHeader when DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var enqueuedAt):
                    message.EnqueuedAt = enqueuedAt;
                    break;
                case ScheduledAtHeader when DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var scheduledAt):
                    message.ScheduledAt = scheduledAt;
                    break;
                case SessionIdHeader:
                    message.SessionId = value;
                    break;
                case PartitionKeyHeader:
                    message.PartitionKey = value;
                    break;
                default:
                    message.Headers[header.Key] = value;
                    break;
            }
        }

        return message;
    }

    internal static string CreateDeliveryTag(string topic, int partition, long offset, string consumerGroup) =>
        $"{topic}|{partition}|{offset}|{consumerGroup}";

    internal static bool TryParseDeliveryTag(
        string deliveryTag,
        out string topic,
        out int partition,
        out long offset,
        out string consumerGroup)
    {
        topic = string.Empty;
        partition = 0;
        offset = 0;
        consumerGroup = string.Empty;

        var parts = deliveryTag.Split('|');
        if (parts.Length != 4)
        {
            return false;
        }

        topic = parts[0];
        consumerGroup = parts[3];
        return int.TryParse(parts[1], out partition) && long.TryParse(parts[2], out offset);
    }

    private static void AddHeader(Headers headers, string key, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        headers.Add(key, Encoding.UTF8.GetBytes(value));
    }

    private static bool IsReservedHeader(string key) =>
        key.Equals(MessageIdHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(CorrelationIdHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(ReplyToHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(RoutingKeyHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(ContentTypeHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(DeliveryCountHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(EnqueuedAtHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(ScheduledAtHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(SessionIdHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(PartitionKeyHeader, StringComparison.OrdinalIgnoreCase);
}
