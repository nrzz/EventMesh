using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Plugins;
using EventMesh.Plugin.Sdk;
using EventMesh.Plugin.Sdk.Filters;
using ZstdSharp;

namespace EventMesh.Plugin.Compression.Zstd;

/// <summary>
/// Zstandard compression plugin for EventMesh message payloads.
/// </summary>
public sealed class ZstdCompressionPlugin : PluginBase, ICompressionPlugin
{
    private static readonly Version PluginVersion = new(1, 0, 0);
    private static readonly Version MinHostVersion = new(0, 1, 0);
    private readonly int _level;

    public ZstdCompressionPlugin(int level = 3)
    {
        _level = level;
    }

    /// <inheritdoc />
    public override PluginManifest Manifest { get; } = new()
    {
        Name = "zstd-compression",
        Version = PluginVersion,
        Description = "Zstandard payload compression for EventMesh messages.",
        Author = "EventMesh",
        MinimumEventMeshVersion = MinHostVersion,
        AssemblyName = typeof(ZstdCompressionPlugin).Assembly.GetName().Name,
        EntryPointType = typeof(ZstdCompressionPlugin).FullName,
        Tags = ["compression", "zstd"],
    };

    /// <inheritdoc />
    public string Algorithm => "zstd";

    /// <inheritdoc />
    public byte[] Compress(ReadOnlySpan<byte> data)
    {
        using var compressor = new Compressor(_level);
        return compressor.Wrap(data).ToArray();
    }

    /// <inheritdoc />
    public byte[] Decompress(ReadOnlySpan<byte> data)
    {
        using var decompressor = new Decompressor();
        return decompressor.Unwrap(data).ToArray();
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
            ["level"] = _level.ToString(),
        });
    }
}
