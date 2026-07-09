namespace EventMesh.Security.Secrets;

/// <summary>
/// HashiCorp Vault secret provider stub that reads connection settings from environment variables.
/// </summary>
public sealed class VaultSecretProvider : ISecretProvider
{
    private readonly HttpClient _httpClient;
    private readonly string? _vaultAddress;
    private readonly string? _vaultToken;

    public VaultSecretProvider(HttpClient? httpClient = null)
    {
        _httpClient = httpClient ?? new HttpClient();
        _vaultAddress = Environment.GetEnvironmentVariable("VAULT_ADDR");
        _vaultToken = Environment.GetEnvironmentVariable("VAULT_TOKEN");
    }

    /// <summary>
    /// Gets the configured Vault address, if present.
    /// </summary>
    public string? VaultAddress => _vaultAddress;

    /// <inheritdoc />
    public async Task<string?> GetSecretAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (string.IsNullOrWhiteSpace(_vaultAddress))
        {
            return Environment.GetEnvironmentVariable($"VAULT_SECRET_{key.Replace('/', '_').ToUpperInvariant()}");
        }

        using var request = new HttpRequestMessage(HttpMethod.Get, $"{_vaultAddress.TrimEnd('/')}/v1/{key}");
        if (!string.IsNullOrWhiteSpace(_vaultToken))
        {
            request.Headers.Add("X-Vault-Token", _vaultToken);
        }

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var payload = await response.Content.ReadAsStringAsync(cancellationToken);
        return ExtractSecretValue(payload);
    }

    private static string? ExtractSecretValue(string payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return null;
        }

        using var document = System.Text.Json.JsonDocument.Parse(payload);
        if (document.RootElement.TryGetProperty("data", out var data) &&
            data.TryGetProperty("data", out var nestedData) &&
            nestedData.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            foreach (var property in nestedData.EnumerateObject())
            {
                if (property.Value.ValueKind == System.Text.Json.JsonValueKind.String)
                {
                    return property.Value.GetString();
                }
            }
        }

        return null;
    }
}
