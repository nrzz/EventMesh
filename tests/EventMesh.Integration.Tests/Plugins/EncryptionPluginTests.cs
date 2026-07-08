using System.Security.Cryptography;
using EventMesh.Plugin.Encryption.AesGcm;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Plugins;

public sealed class EncryptionPluginTests
{
    [Fact]
    public void AesGcm_plugin_encrypts_and_decrypts_payloads()
    {
        var key = RandomNumberGenerator.GetBytes(32);
        var plugin = new AesGcmEncryptionPlugin(key);
        var nonce = RandomNumberGenerator.GetBytes(plugin.NonceSize);
        var payload = "eventmesh aes-gcm encryption sample"u8.ToArray();

        var ciphertext = plugin.Encrypt(payload, nonce, key);
        var plaintext = plugin.Decrypt(ciphertext, nonce, key);

        ciphertext.Should().NotBeEquivalentTo(payload);
        plaintext.Should().BeEquivalentTo(payload);
    }
}
