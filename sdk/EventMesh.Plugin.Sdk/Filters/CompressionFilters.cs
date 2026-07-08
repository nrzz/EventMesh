using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Pipeline;

namespace EventMesh.Plugin.Sdk.Filters;

/// <summary>
/// Compresses message envelope payloads during publish.
/// </summary>
public sealed class CompressionPublishFilter<T> : IPublishFilter<T> where T : notnull
{
    private readonly ICompressionPlugin _compression;

    public CompressionPublishFilter(ICompressionPlugin compression)
    {
        _compression = compression;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Envelope?.Data is { Length: > 0 } data)
        {
            var compressed = _compression.Compress(data.Span);
            var headers = new Dictionary<string, string>(context.Envelope.Headers, StringComparer.OrdinalIgnoreCase)
            {
                ["eventmesh-content-encoding"] = _compression.Algorithm,
            };

            context.Envelope = MessageEnvelope.From(context.Envelope)
                .WithData(compressed)
                .WithHeaders(headers)
                .Build();
        }

        await next(context, cancellationToken);
    }
}

/// <summary>
/// Decompresses message envelope payloads during consume.
/// </summary>
public sealed class CompressionConsumeFilter<T> : IConsumeFilter<T> where T : notnull
{
    private readonly ICompressionPlugin _compression;

    public CompressionConsumeFilter(ICompressionPlugin compression)
    {
        _compression = compression;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Envelope.Headers.TryGetValue("eventmesh-content-encoding", out var encoding) &&
            string.Equals(encoding, _compression.Algorithm, StringComparison.OrdinalIgnoreCase) &&
            context.Envelope.Data is { Length: > 0 } data)
        {
            var decompressed = _compression.Decompress(data.Span);
            context.Envelope = MessageEnvelope.From(context.Envelope)
                .WithData(decompressed)
                .Build();
        }

        await next(context, cancellationToken);
    }
}
