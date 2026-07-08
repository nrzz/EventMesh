using System.Globalization;
using System.Text;
using Amazon.SQS.Model;
using EventMesh.Abstractions.Transport;

namespace EventMesh.Transport.AmazonSqs;

internal static class AmazonSqsMessageCodec
{
    public const string CorrelationIdAttribute = "x-eventmesh-correlation-id";
    public const string ReplyToAttribute = "x-eventmesh-reply-to";
    public const string RoutingKeyAttribute = "x-eventmesh-routing-key";
    public const string ContentTypeAttribute = "x-eventmesh-content-type";
    public const string ApplicationMessageIdAttribute = "x-eventmesh-message-id";
    public const string EnqueuedAtAttribute = "x-eventmesh-enqueued-at";
    public const string BodyEncodingAttribute = "x-eventmesh-body-encoding";
    public const string HeaderAttributePrefix = "x-eventmesh-h-";
    public const string DeadLetterReasonHeader = "x-eventmesh-dead-letter-reason";
    public const string BodyEncodingBase64 = "base64";

    public static SendMessageRequest ToSendRequest(
        string queueUrl,
        TransportMessage message,
        bool fifo,
        int? delaySeconds)
    {
        var request = new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = EncodeBody(message.Body),
            MessageAttributes = ToMessageAttributes(message),
        };

        if (delaySeconds is not null)
        {
            request.DelaySeconds = delaySeconds.Value;
        }

        if (fifo)
        {
            request.MessageGroupId = message.SessionId
                ?? message.PartitionKey
                ?? message.MessageId
                ?? Guid.NewGuid().ToString("N");
            request.MessageDeduplicationId = message.MessageId ?? Guid.NewGuid().ToString("N");
        }

        return request;
    }

    public static TransportMessage FromSqsMessage(Message message, string destination)
    {
        var attributes = message.MessageAttributes ?? new Dictionary<string, MessageAttributeValue>();
        var headers = ExtractHeaders(attributes);
        var bodyEncoding = GetAttribute(attributes, BodyEncodingAttribute);
        var deliveryCount = 0;
        if (message.Attributes is not null
            && message.Attributes.TryGetValue("ApproximateReceiveCount", out var receiveCount)
            && int.TryParse(receiveCount, out var parsedCount))
        {
            deliveryCount = Math.Max(0, parsedCount - 1);
        }

        return new TransportMessage
        {
            MessageId = GetAttribute(attributes, ApplicationMessageIdAttribute) ?? message.MessageId,
            Destination = destination,
            RoutingKey = GetAttribute(attributes, RoutingKeyAttribute),
            Body = DecodeBody(message.Body, bodyEncoding),
            ContentType = GetAttribute(attributes, ContentTypeAttribute),
            Headers = headers,
            CorrelationId = GetAttribute(attributes, CorrelationIdAttribute),
            ReplyTo = GetAttribute(attributes, ReplyToAttribute),
            DeliveryCount = deliveryCount,
            EnqueuedAt = ParseEnqueuedAt(attributes) ?? DateTimeOffset.UtcNow,
        };
    }

    public static Dictionary<string, MessageAttributeValue> ToMessageAttributes(TransportMessage message)
    {
        var attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.OrdinalIgnoreCase);

        AddStringAttribute(attributes, ApplicationMessageIdAttribute, message.MessageId);
        AddStringAttribute(attributes, CorrelationIdAttribute, message.CorrelationId);
        AddStringAttribute(attributes, ReplyToAttribute, message.ReplyTo);
        AddStringAttribute(attributes, RoutingKeyAttribute, message.RoutingKey);
        AddStringAttribute(attributes, ContentTypeAttribute, message.ContentType);
        AddStringAttribute(attributes, EnqueuedAtAttribute, message.EnqueuedAt?.ToString("O", CultureInfo.InvariantCulture));
        AddStringAttribute(attributes, BodyEncodingAttribute, BodyEncodingBase64);

        foreach (var header in message.Headers)
        {
            AddStringAttribute(attributes, HeaderAttributePrefix + header.Key, header.Value);
        }

        return attributes;
    }

    private static Dictionary<string, string> ExtractHeaders(IReadOnlyDictionary<string, MessageAttributeValue> attributes)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var attribute in attributes)
        {
            if (!attribute.Key.StartsWith(HeaderAttributePrefix, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var headerName = attribute.Key[HeaderAttributePrefix.Length..];
            headers[headerName] = attribute.Value.StringValue ?? string.Empty;
        }

        return headers;
    }

    private static string EncodeBody(ReadOnlyMemory<byte> body) =>
        body.Length == 0 ? string.Empty : Convert.ToBase64String(body.Span);

    private static byte[] DecodeBody(string body, string? encoding) =>
        string.Equals(encoding, BodyEncodingBase64, StringComparison.OrdinalIgnoreCase)
            ? (string.IsNullOrEmpty(body) ? [] : Convert.FromBase64String(body))
            : Encoding.UTF8.GetBytes(body);

    private static DateTimeOffset? ParseEnqueuedAt(IReadOnlyDictionary<string, MessageAttributeValue> attributes)
    {
        var value = GetAttribute(attributes, EnqueuedAtAttribute);
        return value is not null && DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : null;
    }

    private static string? GetAttribute(IReadOnlyDictionary<string, MessageAttributeValue> attributes, string name) =>
        attributes.TryGetValue(name, out var value) ? value.StringValue : null;

    private static void AddStringAttribute(
        IDictionary<string, MessageAttributeValue> attributes,
        string name,
        string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        attributes[name] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = value,
        };
    }
}
