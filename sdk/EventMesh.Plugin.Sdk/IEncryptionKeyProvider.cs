namespace EventMesh.Plugin.Sdk;

/// <summary>
/// Supplies encryption keys to encryption pipeline filters.
/// </summary>
public interface IEncryptionKeyProvider
{
    /// <summary>
    /// Gets the symmetric encryption key bytes.
    /// </summary>
    byte[] GetKey();
}
