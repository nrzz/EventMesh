using System.IO.Compression;
using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Plugins;
using EventMesh.Plugin.Sdk;
using EventMesh.Plugin.Sdk.Filters;

namespace EventMesh.Plugin.Compression.Gzip;

/// <summary>
/// gzip compression plugin for EventMesh message payloads.
/// </summary>
public sealed class GzipCompressionPlugin : PluginBase, ICompressionPlugin
{
    private static readonly Version PluginVersion = new(1, 0, 0);
    private static readonly Version MinHostVersion = new(0, 1, 0);

    /// <inheritdoc />
    public override PluginManifest Manifest { get; } = new()
    {
        Name = "gzip-compression",
        Version = PluginVersion,
        Description = "gzip payload compression for EventMesh messages.",
        Author = "EventMesh",
        MinimumEventMeshVersion = MinHostVersion,
        AssemblyName = typeof(GzipCompressionPlugin).Assembly.GetName().Name,
        EntryPointType = typeof(GzipCompressionPlugin).FullName,
        Tags = ["compression", "gzip"],
    };

    /// <inheritdoc />
    public string Algorithm => "gzip";

    /// <inheritdoc />
    public byte[] Compress(ReadOnlySpan<byte> data)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            gzip.Write(data);
        }

        return output.ToArray();
    }

    /// <inheritdoc />
    public byte[] Decompress(ReadOnlySpan<byte> data)
    {
        using var input = new MemoryStream(data.ToArray());
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return output.ToArray();
    }

    /// <inheritdoc />
    public override void ConfigurePlugin(IPluginBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        base.ConfigurePlugin(builder);
        builder.AddSingleton<ICompressionPlugin>(this);
        builder.AddPublishFilter(typeof(CompressionPublishFilter<>));
        builder.AddConsumeFilter(typeof(CompressionConsumeFilter<>));
    }

    /// <inheritdoc />
    public override void Configure(EventMeshOptions options)
    {
        options.PluginSettings.TryAdd(Name, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["algorithm"] = Algorithm,
        });
    }
}
