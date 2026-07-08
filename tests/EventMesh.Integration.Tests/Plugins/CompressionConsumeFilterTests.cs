using EventMesh.Plugin.Compression.Gzip;
using EventMesh.Plugin.Sdk.Filters;
using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Pipeline;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Plugins;

public sealed class CompressionConsumeFilterTests
{
    [Fact]
    public async Task Gzip_consume_filter_decompresses_envelope_data()
    {
        var compression = new GzipCompressionPlugin();
        var consumeFilter = new CompressionConsumeFilter<string>(compression);
        var original = "roundtrip payload"u8.ToArray();
        var compressed = compression.Compress(original);

        var envelope = MessageEnvelope.Create()
            .WithSource("eventmesh.tests")
            .WithType("test.message")
            .WithData(compressed)
            .WithHeader("eventmesh-content-encoding", compression.Algorithm)
            .Build();

        var context = new ConsumeContext<string>
        {
            Message = "payload",
            Envelope = envelope,
        };

        await consumeFilter.FilterAsync(context, (ctx, _) =>
        {
            ctx.Envelope.Data!.Value.ToArray().Should().BeEquivalentTo(original);
            return Task.CompletedTask;
        });
    }
}
