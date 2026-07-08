using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Plugin.Compression.Gzip;
using EventMesh.Plugin.Sdk;
using EventMesh.Plugin.Sdk.Filters;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Plugins;

public sealed class CompressionPipelineIntegrationTests
{
    [Fact]
    public async Task Gzip_plugin_compresses_envelope_data_in_publish_pipeline()
    {
        var compression = new GzipCompressionPlugin();
        var publishFilter = new CompressionPublishFilter<string>(compression);

        var context = new PublishContext<string>
        {
            Message = "payload",
            Envelope = MessageEnvelope.Create()
                .WithSource("eventmesh.tests")
                .WithType("test.message")
                .WithData("hello eventmesh"u8.ToArray())
                .Build(),
        };

        await publishFilter.FilterAsync(context, (ctx, _) =>
        {
            ctx.Envelope.Should().NotBeNull();
            ctx.Envelope!.Headers.Should().ContainKey("eventmesh-content-encoding");
            ctx.Envelope.Headers["eventmesh-content-encoding"].Should().Be(compression.Algorithm);
            var decompressed = compression.Decompress(ctx.Envelope.Data!.Value.Span);
            decompressed.Should().BeEquivalentTo("hello eventmesh"u8.ToArray());
            return Task.CompletedTask;
        });
    }
}
