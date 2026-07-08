using EventMesh.Abstractions.Plugins;
using EventMesh.Plugin.Compression.Gzip;
using EventMesh.Plugin.Compression.Zstd;
using EventMesh.Plugin.Encryption.AesGcm;
using EventMesh.Plugin.Exporter.Prometheus;
using EventMesh.Plugin.Sdk;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Plugins;

public sealed class PluginManifestTests
{
    [Fact]
    public void All_first_party_plugins_expose_versioned_manifests()
    {
        IEventMeshPlugin[] plugins =
        [
            new GzipCompressionPlugin(),
            new ZstdCompressionPlugin(),
            new AesGcmEncryptionPlugin(CreateTestKey()),
            new PrometheusExporterPlugin(),
        ];

        foreach (var plugin in plugins)
        {
            plugin.Should().BeAssignableTo<PluginBase>();
            var manifest = ((PluginBase)plugin).Manifest;
            manifest.Name.Should().NotBeNullOrWhiteSpace();
            manifest.Version.Should().NotBeNull();
            manifest.MinimumEventMeshVersion.Should().NotBeNull();
            manifest.IsCompatibleWith(new Version(0, 1, 0)).Should().BeTrue();
        }
    }

    private static byte[] CreateTestKey() =>
        Convert.FromBase64String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
}
