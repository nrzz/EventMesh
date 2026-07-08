using System.Globalization;
using System.Text;
using EventMesh.Abstractions.Transport;
using NATS.Client.Core;

namespace EventMesh.Transport.Nats;

internal static class NatsMessageCodec
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

    internal static NatsHeaders ToNatsHeaders(TransportMessage message)
    {
        var headers = new NatsHeaders();

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

    internal static TransportMessage FromNatsMessage(string destination, ReadOnlyMemory<byte> body, NatsHeaders? headers, ulong deliveryCount = 0)
    {
        var message = new TransportMessage
        {
            Destination = destination,
            Body = body,
            Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            DeliveryCount = deliveryCount > int.MaxValue ? int.MaxValue : (int)deliveryCount,
        };

        if (headers is null)
        {
            return message;
        }

        foreach (var header in headers)
        {
            var value = header.Value.ToString();
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
                case DeliveryCountHeader when int.TryParse(value, out var parsedDeliveryCount):
                    message.DeliveryCount = parsedDeliveryCount;
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

    internal static string CreateDeliveryTag(string streamName, string consumerName, ulong sequence, string subject) =>
        $"{streamName}|{consumerName}|{sequence}|{subject}";

    internal static bool TryParseDeliveryTag(
        string deliveryTag,
        out string streamName,
        out string consumerName,
        out ulong sequence,
        out string subject)
    {
        streamName = string.Empty;
        consumerName = string.Empty;
        sequence = 0;
        subject = string.Empty;

        var parts = deliveryTag.Split('|');
        if (parts.Length != 4)
        {
            return false;
        }

        streamName = parts[0];
        consumerName = parts[1];
        subject = parts[3];
        return ulong.TryParse(parts[2], out sequence);
    }

    private static void AddHeader(NatsHeaders headers, string key, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        headers[key] = value;
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
