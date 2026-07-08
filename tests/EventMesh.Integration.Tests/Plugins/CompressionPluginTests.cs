using EventMesh.Plugin.Compression.Gzip;
using EventMesh.Plugin.Compression.Zstd;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Plugins;

public sealed class CompressionPluginTests
{
    [Fact]
    public void Gzip_plugin_roundtrips_payloads()
    {
        var plugin = new GzipCompressionPlugin();
        var payload = "eventmesh gzip compression sample"u8.ToArray();

        var compressed = plugin.Compress(payload);
        var decompressed = plugin.Decompress(compressed);

        compressed.Should().NotBeEquivalentTo(payload);
        decompressed.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public void Zstd_plugin_roundtrips_payloads()
    {
        var plugin = new ZstdCompressionPlugin(level: 3);
        var payload = "eventmesh zstd compression sample"u8.ToArray();

        var compressed = plugin.Compress(payload);
        var decompressed = plugin.Decompress(compressed);

        compressed.Should().NotBeEquivalentTo(payload);
        decompressed.Should().BeEquivalentTo(payload);
    }
}
