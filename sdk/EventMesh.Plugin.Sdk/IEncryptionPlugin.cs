namespace EventMesh.Plugin.Sdk;

/// <summary>
/// Contract for message payload encryption algorithms.
/// </summary>
public interface IEncryptionPlugin
{
    /// <summary>
    /// Gets the encryption algorithm identifier (for example, aes-256-gcm).
    /// </summary>
    string Algorithm { get; }

    /// <summary>
    /// Gets the required nonce size in bytes for this algorithm.
    /// </summary>
    int NonceSize { get; }

    /// <summary>
    /// Gets the authentication tag size in bytes for this algorithm.
    /// </summary>
    int TagSize { get; }

    /// <summary>
    /// Encrypts plaintext and returns ciphertext with the authentication tag appended.
    /// </summary>
    byte[] Encrypt(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> key);

    /// <summary>
    /// Decrypts ciphertext that includes the authentication tag.
    /// </summary>
    byte[] Decrypt(ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> key);
}
