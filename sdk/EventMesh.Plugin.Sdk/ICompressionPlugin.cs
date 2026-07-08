namespace EventMesh.Plugin.Sdk;

/// <summary>
/// Contract for message payload compression algorithms.
/// </summary>
public interface ICompressionPlugin
{
    /// <summary>
    /// Gets the compression algorithm identifier (for example, gzip or zstd).
    /// </summary>
    string Algorithm { get; }

    /// <summary>
    /// Compresses the supplied payload bytes.
    /// </summary>
    byte[] Compress(ReadOnlySpan<byte> data);

    /// <summary>
    /// Decompresses the supplied payload bytes.
    /// </summary>
    byte[] Decompress(ReadOnlySpan<byte> data);
}
