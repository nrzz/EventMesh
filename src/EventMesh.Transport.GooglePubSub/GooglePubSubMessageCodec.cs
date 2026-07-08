using System.Globalization;
using EventMesh.Abstractions.Transport;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

namespace EventMesh.Transport.GooglePubSub;

internal static class GooglePubSubMessageCodec
{
    internal const string MessageIdAttribute = "x-eventmesh-message-id";
    internal const string CorrelationIdAttribute = "x-eventmesh-correlation-id";
    internal const string ReplyToAttribute = "x-eventmesh-reply-to";
    internal const string RoutingKeyAttribute = "x-eventmesh-routing-key";
    internal const string ContentTypeAttribute = "content-type";
    internal const string DeliveryCountAttribute = "x-eventmesh-delivery-count";
    internal const string EnqueuedAtAttribute = "x-eventmesh-enqueued-at";
    internal const string ScheduledAtAttribute = "x-eventmesh-scheduled-at";
    internal const string SessionIdAttribute = "x-eventmesh-session-id";
    internal const string PartitionKeyAttribute = "x-eventmesh-partition-key";
    internal const string DeadLetterReasonHeader = "x-eventmesh-dead-letter-reason";
    internal const string ReplayHeader = "x-eventmesh-replay";

    internal static PubsubMessage ToPubsubMessage(TransportMessage message)
    {
        var pubsubMessage = new PubsubMessage
        {
            Data = ByteString.CopyFrom(message.Body.Span),
            OrderingKey = message.PartitionKey ?? message.SessionId ?? string.Empty,
        };

        AddAttribute(pubsubMessage, MessageIdAttribute, message.MessageId);
        AddAttribute(pubsubMessage, CorrelationIdAttribute, message.CorrelationId);
        AddAttribute(pubsubMessage, ReplyToAttribute, message.ReplyTo);
        AddAttribute(pubsubMessage, RoutingKeyAttribute, message.RoutingKey);
        AddAttribute(pubsubMessage, ContentTypeAttribute, message.ContentType);
        AddAttribute(pubsubMessage, SessionIdAttribute, message.SessionId);
        AddAttribute(pubsubMessage, PartitionKeyAttribute, message.PartitionKey ?? message.SessionId);
        AddAttribute(pubsubMessage, DeliveryCountAttribute, message.DeliveryCount.ToString(CultureInfo.InvariantCulture));

        if (message.EnqueuedAt is not null)
        {
            AddAttribute(pubsubMessage, EnqueuedAtAttribute, message.EnqueuedAt.Value.ToString("O", CultureInfo.InvariantCulture));
        }

        if (message.ScheduledAt is not null)
        {
            AddAttribute(pubsubMessage, ScheduledAtAttribute, message.ScheduledAt.Value.ToString("O", CultureInfo.InvariantCulture));
        }

        foreach (var header in message.Headers)
        {
            if (IsReservedAttribute(header.Key))
            {
                continue;
            }

            AddAttribute(pubsubMessage, header.Key, header.Value);
        }

        return pubsubMessage;
    }

    internal static TransportMessage FromPubsubMessage(string destination, PubsubMessage pubsubMessage)
    {
        var message = new TransportMessage
        {
            Destination = destination,
            Body = pubsubMessage.Data.ToByteArray(),
            Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            MessageId = pubsubMessage.MessageId,
        };

        foreach (var (key, value) in pubsubMessage.Attributes)
        {
            switch (key)
            {
                case MessageIdAttribute:
                    message.MessageId = value;
                    break;
                case CorrelationIdAttribute:
                    message.CorrelationId = value;
                    break;
                case ReplyToAttribute:
                    message.ReplyTo = value;
                    break;
                case RoutingKeyAttribute:
                    message.RoutingKey = value;
                    break;
                case ContentTypeAttribute:
                    message.ContentType = value;
                    break;
                case DeliveryCountAttribute when int.TryParse(value, out var deliveryCount):
                    message.DeliveryCount = deliveryCount;
                    break;
                case EnqueuedAtAttribute when DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var enqueuedAt):
                    message.EnqueuedAt = enqueuedAt;
                    break;
                case ScheduledAtAttribute when DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var scheduledAt):
                    message.ScheduledAt = scheduledAt;
                    break;
                case SessionIdAttribute:
                    message.SessionId = value;
                    break;
                case PartitionKeyAttribute:
                    message.PartitionKey = value;
                    break;
                default:
                    message.Headers[key] = value;
                    break;
            }
        }

        if (pubsubMessage.PublishTime is not null)
        {
            message.EnqueuedAt ??= pubsubMessage.PublishTime.ToDateTimeOffset();
        }

        return message;
    }

    internal static string CreateDeliveryTag(string subscriptionId, string ackId, string messageId) =>
        $"{subscriptionId}|{ackId}|{messageId}";

    internal static bool TryParseDeliveryTag(
        string deliveryTag,
        out string subscriptionId,
        out string ackId,
        out string messageId)
    {
        subscriptionId = string.Empty;
        ackId = string.Empty;
        messageId = string.Empty;

        var separatorIndex = deliveryTag.IndexOf('|', StringComparison.Ordinal);
        if (separatorIndex <= 0)
        {
            return false;
        }

        subscriptionId = deliveryTag[..separatorIndex];

        var remainder = deliveryTag[(separatorIndex + 1)..];
        var secondSeparator = remainder.IndexOf('|', StringComparison.Ordinal);
        if (secondSeparator <= 0)
        {
            return false;
        }

        ackId = remainder[..secondSeparator];
        messageId = remainder[(secondSeparator + 1)..];
        return !string.IsNullOrWhiteSpace(subscriptionId)
            && !string.IsNullOrWhiteSpace(ackId)
            && !string.IsNullOrWhiteSpace(messageId);
    }

    internal static string BuildSubscriptionFilter(string? filter) =>
        string.IsNullOrWhiteSpace(filter) || filter is "#" or "*"
            ? string.Empty
            : $"attributes.{RoutingKeyAttribute} = \"{filter.Replace("\"", "\\\"", StringComparison.Ordinal)}\"";

    private static void AddAttribute(PubsubMessage message, string key, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        message.Attributes[key] = value;
    }

    private static bool IsReservedAttribute(string key) =>
        key.Equals(MessageIdAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(CorrelationIdAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(ReplyToAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(RoutingKeyAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(ContentTypeAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(DeliveryCountAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(EnqueuedAtAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(ScheduledAtAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(SessionIdAttribute, StringComparison.OrdinalIgnoreCase)
        || key.Equals(PartitionKeyAttribute, StringComparison.OrdinalIgnoreCase);
}
