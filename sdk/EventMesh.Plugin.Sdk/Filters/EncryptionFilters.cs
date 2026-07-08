using System.Security.Cryptography;
using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Pipeline;

namespace EventMesh.Plugin.Sdk.Filters;

/// <summary>
/// Encrypts message envelope payloads during publish.
/// </summary>
public sealed class EncryptionPublishFilter<T> : IPublishFilter<T> where T : notnull
{
    private readonly IEncryptionPlugin _encryption;
    private readonly IEncryptionKeyProvider _keyProvider;

    public EncryptionPublishFilter(IEncryptionPlugin encryption, IEncryptionKeyProvider keyProvider)
    {
        _encryption = encryption;
        _keyProvider = keyProvider;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Envelope?.Data is { Length: > 0 } data)
        {
            var nonce = RandomNumberGenerator.GetBytes(_encryption.NonceSize);
            var key = _keyProvider.GetKey();
            var ciphertext = _encryption.Encrypt(data.Span, nonce, key);
            var headers = new Dictionary<string, string>(context.Envelope.Headers, StringComparer.OrdinalIgnoreCase)
            {
                ["eventmesh-content-encryption"] = _encryption.Algorithm,
                ["eventmesh-encryption-nonce"] = Convert.ToBase64String(nonce),
            };

            context.Envelope = MessageEnvelope.From(context.Envelope)
                .WithData(ciphertext)
                .WithHeaders(headers)
                .Build();
        }

        await next(context, cancellationToken);
    }
}

/// <summary>
/// Decrypts message envelope payloads during consume.
/// </summary>
public sealed class EncryptionConsumeFilter<T> : IConsumeFilter<T> where T : notnull
{
    private readonly IEncryptionPlugin _encryption;
    private readonly IEncryptionKeyProvider _keyProvider;

    public EncryptionConsumeFilter(IEncryptionPlugin encryption, IEncryptionKeyProvider keyProvider)
    {
        _encryption = encryption;
        _keyProvider = keyProvider;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Envelope.Headers.TryGetValue("eventmesh-content-encryption", out var algorithm) &&
            string.Equals(algorithm, _encryption.Algorithm, StringComparison.OrdinalIgnoreCase) &&
            context.Envelope.Headers.TryGetValue("eventmesh-encryption-nonce", out var nonceValue) &&
            context.Envelope.Data is { Length: > 0 } data)
        {
            var nonce = Convert.FromBase64String(nonceValue);
            var key = _keyProvider.GetKey();
            var plaintext = _encryption.Decrypt(data.Span, nonce, key);
            context.Envelope = MessageEnvelope.From(context.Envelope)
                .WithData(plaintext)
                .Build();
        }

        await next(context, cancellationToken);
    }
}
