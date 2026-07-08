using EventMesh.Plugin.Encryption.AesGcm;
using EventMesh.Plugin.Sdk;
using EventMesh.Plugin.Sdk.Filters;
using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Pipeline;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Plugins;

public sealed class EncryptionPipelineIntegrationTests
{
    [Fact]
    public async Task AesGcm_filters_encrypt_and_decrypt_envelope_data()
    {
        var key = Convert.FromBase64String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        var plugin = new AesGcmEncryptionPlugin(key);
        var keyProvider = new StaticKeyProvider(key);
        var publishFilter = new EncryptionPublishFilter<string>(plugin, keyProvider);
        var consumeFilter = new EncryptionConsumeFilter<string>(plugin, keyProvider);

        var context = new PublishContext<string>
        {
            Message = "payload",
            Envelope = MessageEnvelope.Create()
                .WithSource("eventmesh.tests")
                .WithType("test.message")
                .WithData("secret payload"u8.ToArray())
                .Build(),
        };

        PublishContext<string>? encryptedContext = null;
        await publishFilter.FilterAsync(context, (ctx, _) =>
        {
            encryptedContext = ctx;
            return Task.CompletedTask;
        });

        encryptedContext.Should().NotBeNull();
        encryptedContext!.Envelope!.Headers.Should().ContainKey("eventmesh-content-encryption");
        encryptedContext.Envelope.Headers.Should().ContainKey("eventmesh-encryption-nonce");

        var consumeContext = new ConsumeContext<string>
        {
            Message = "payload",
            Envelope = encryptedContext.Envelope!,
        };

        await consumeFilter.FilterAsync(consumeContext, (ctx, _) =>
        {
            ctx.Envelope.Data.Should().NotBeNull();
            ctx.Envelope.Data!.Value.ToArray().Should().BeEquivalentTo("secret payload"u8.ToArray());
            return Task.CompletedTask;
        });
    }

    private sealed class StaticKeyProvider : IEncryptionKeyProvider
    {
        private readonly byte[] _key;

        public StaticKeyProvider(byte[] key) => _key = key.ToArray();

        public byte[] GetKey() => _key.ToArray();
    }
}
