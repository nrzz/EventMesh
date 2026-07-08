using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Reliability;
using EventMesh.Abstractions.Transport;
using EventMesh.Core.Capabilities;
using EventMesh.Core.Internal;
using EventMesh.Core.Observability;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Routes failed messages to a dead-letter destination when delivery attempts are exhausted.
/// </summary>
public sealed class DeadLetterFilter<T> : IConsumeFilter<T> where T : notnull
{
    private readonly IBrokerTransport _transport;
    private readonly CapabilityEmulator _capabilityEmulator;
    private readonly EventMeshOptions _meshOptions;
    private readonly EventMeshMetrics _metrics;
    private readonly ILogger<DeadLetterFilter<T>> _logger;

    public DeadLetterFilter(
        IBrokerTransport transport,
        CapabilityEmulator capabilityEmulator,
        IOptions<EventMeshOptions> meshOptions,
        EventMeshMetrics metrics,
        ILogger<DeadLetterFilter<T>> logger)
    {
        _transport = transport;
        _capabilityEmulator = capabilityEmulator;
        _meshOptions = meshOptions.Value;
        _metrics = metrics;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        var deadLetterOptions = context.Options.DeadLetter?.Clone()
            ?? _meshOptions.DefaultDeadLetterOptions?.Clone();

        if (deadLetterOptions is null)
        {
            await next(context, cancellationToken);
            return;
        }

        try
        {
            await next(context, cancellationToken);
        }
        catch (Exception ex)
        {
            var maxAttempts = deadLetterOptions.MaxDeliveryAttempts;
            if (context.DeliveryCount + 1 < maxAttempts)
            {
                throw;
            }

            var sourceDestination = context.Options.Topic ?? context.Envelope.Type;
            var deadLetterDestination = deadLetterOptions.Destination
                ?? _capabilityEmulator.ResolveDeadLetterDestination(sourceDestination);

            var envelope = context.Envelope;
            if (deadLetterOptions.IncludeFailureReason)
            {
                var headers = new Dictionary<string, string>(envelope.Headers, StringComparer.OrdinalIgnoreCase)
                {
                    [deadLetterOptions.FailureReasonHeader] = ex.Message,
                };
                envelope = envelope.WithHeaders(headers);
            }

            if (deadLetterOptions.Headers is not null)
            {
                var headers = new Dictionary<string, string>(envelope.Headers, StringComparer.OrdinalIgnoreCase);
                foreach (var header in deadLetterOptions.Headers)
                {
                    headers[header.Key] = header.Value;
                }

                envelope = envelope.WithHeaders(headers);
            }

            var transportMessage = EnvelopeMapper.ToTransportMessage(envelope, deadLetterDestination);
            var result = await _transport.SendAsync(transportMessage, cancellationToken);
            if (!result.Succeeded)
            {
                _logger.LogError(
                    "Failed to dead-letter message {MessageId} to {Destination}: {Error}",
                    envelope.Id,
                    deadLetterDestination,
                    result.ErrorMessage);
                throw;
            }

            _metrics.RecordDeadLettered(sourceDestination, envelope.Type);
            _logger.LogWarning(
                ex,
                "Message {MessageId} dead-lettered to {Destination} after {DeliveryCount} attempts.",
                envelope.Id,
                deadLetterDestination,
                context.DeliveryCount + 1);

            context.IsRejected = true;
            if (!string.IsNullOrWhiteSpace(context.DeliveryTag))
            {
                await _transport.RejectAsync(context.DeliveryTag, requeue: false, cancellationToken);
            }
        }
    }
}
