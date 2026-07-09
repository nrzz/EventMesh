namespace EventMesh.Security.Secrets;

/// <summary>
/// Reads secrets from environment variables.
/// </summary>
public sealed class EnvironmentSecretProvider : ISecretProvider
{
    /// <inheritdoc />
    public Task<string?> GetSecretAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return Task.FromResult(Environment.GetEnvironmentVariable(key));
    }
}
