using System.Security.Cryptography;
using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Plugins;
using EventMesh.Plugin.Sdk;
using EventMesh.Plugin.Sdk.Filters;

namespace EventMesh.Plugin.Encryption.AesGcm;

/// <summary>
/// AES-256-GCM encryption plugin for EventMesh message payloads.
/// </summary>
public sealed class AesGcmEncryptionPlugin : PluginBase, IEncryptionPlugin
{
    private const int KeySize = 32;
    private static readonly Version PluginVersion = new(1, 0, 0);
    private static readonly Version MinHostVersion = new(0, 1, 0);
    private readonly byte[] _key;

    public AesGcmEncryptionPlugin(byte[]? key = null)
    {
        _key = key ?? LoadKeyFromEnvironment();
    }

    /// <inheritdoc />
    public override PluginManifest Manifest { get; } = new()
    {
        Name = "aes-gcm-encryption",
        Version = PluginVersion,
        Description = "AES-256-GCM payload encryption for EventMesh messages.",
        Author = "EventMesh",
        MinimumEventMeshVersion = MinHostVersion,
        AssemblyName = typeof(AesGcmEncryptionPlugin).Assembly.GetName().Name,
        EntryPointType = typeof(AesGcmEncryptionPlugin).FullName,
        Tags = ["encryption", "aes-gcm"],
    };

    /// <inheritdoc />
    public string Algorithm => "aes-256-gcm";

    /// <inheritdoc />
    public int NonceSize => 12;

    /// <inheritdoc />
    public int TagSize => 16;

    /// <inheritdoc />
    public byte[] Encrypt(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> key)
    {
        ValidateKey(key);
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[TagSize];
        using var aes = new System.Security.Cryptography.AesGcm(key, TagSize);
        aes.Encrypt(nonce, plaintext, ciphertext, tag);

        var result = new byte[ciphertext.Length + tag.Length];
        ciphertext.CopyTo(result, 0);
        tag.CopyTo(result, ciphertext.Length);
        return result;
    }

    /// <inheritdoc />
    public byte[] Decrypt(ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> key)
    {
        ValidateKey(key);
        if (ciphertext.Length < TagSize)
        {
            throw new CryptographicException("Ciphertext is too short to contain an authentication tag.");
        }

        var payloadLength = ciphertext.Length - TagSize;
        var payload = ciphertext[..payloadLength];
        var tag = ciphertext[payloadLength..];
        var plaintext = new byte[payloadLength];
        using var aes = new System.Security.Cryptography.AesGcm(key, TagSize);
        aes.Decrypt(nonce, payload, tag, plaintext);
        return plaintext;
    }

    /// <inheritdoc />
    public override void ConfigurePlugin(IPluginBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        base.ConfigurePlugin(builder);
        builder.AddSingleton<IEncryptionPlugin>(this);
        builder.AddSingleton<IEncryptionKeyProvider>(new StaticEncryptionKeyProvider(_key));
        builder.AddPublishFilter(typeof(EncryptionPublishFilter<>));
        builder.AddConsumeFilter(typeof(EncryptionConsumeFilter<>));
    }

    /// <inheritdoc />
    public override void Configure(EventMeshOptions options)
    {
        options.PluginSettings.TryAdd(Name, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["algorithm"] = Algorithm,
            ["keySource"] = "environment:EVENTMESH_ENCRYPTION_KEY",
        });
    }

    private static byte[] LoadKeyFromEnvironment()
    {
        var keyValue = Environment.GetEnvironmentVariable("EVENTMESH_ENCRYPTION_KEY");
        if (string.IsNullOrWhiteSpace(keyValue))
        {
            throw new InvalidOperationException(
                "EVENTMESH_ENCRYPTION_KEY must be set to a base64-encoded 32-byte AES key when no key is provided to the plugin constructor.");
        }

        try
        {
            var decoded = Convert.FromBase64String(keyValue);
            ValidateKey(decoded);
            return decoded;
        }
        catch (FormatException ex)
        {
            throw new InvalidOperationException(
                "EVENTMESH_ENCRYPTION_KEY must be a base64-encoded 32-byte AES key.",
                ex);
        }
    }

    private static void ValidateKey(ReadOnlySpan<byte> key)
    {
        if (key.Length != KeySize)
        {
            throw new CryptographicException($"AES-256-GCM requires a {KeySize}-byte key.");
        }
    }

    private sealed class StaticEncryptionKeyProvider : IEncryptionKeyProvider
    {
        private readonly byte[] _key;

        public StaticEncryptionKeyProvider(byte[] key)
        {
            _key = key.ToArray();
        }

        public byte[] GetKey() => _key.ToArray();
    }
}
