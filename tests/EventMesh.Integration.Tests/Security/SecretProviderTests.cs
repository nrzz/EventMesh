using EventMesh.Security.Secrets;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Security;

public sealed class SecretProviderTests
{
    [Fact]
    public async Task EnvironmentSecretProvider_reads_configured_values()
    {
        const string key = "EVENTMESH_TEST_SECRET";
        const string value = "secret-value";
        Environment.SetEnvironmentVariable(key, value);

        try
        {
            var provider = new EnvironmentSecretProvider();
            var secret = await provider.GetSecretAsync(key);
            secret.Should().Be(value);
        }
        finally
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Fact]
    public async Task VaultSecretProvider_reads_fallback_environment_secret_when_vault_is_unconfigured()
    {
        const string key = "encryption/key";
        Environment.SetEnvironmentVariable("VAULT_ADDR", null);
        Environment.SetEnvironmentVariable("VAULT_SECRET_ENCRYPTION_KEY", "vault-fallback-key");

        try
        {
            var provider = new VaultSecretProvider();
            provider.VaultAddress.Should().BeNull();
            var secret = await provider.GetSecretAsync(key);
            secret.Should().Be("vault-fallback-key");
        }
        finally
        {
            Environment.SetEnvironmentVariable("VAULT_SECRET_ENCRYPTION_KEY", null);
        }
    }
}
