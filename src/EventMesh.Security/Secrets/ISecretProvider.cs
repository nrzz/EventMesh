namespace EventMesh.Security.Secrets;

/// <summary>
/// Retrieves secrets from external stores.
/// </summary>
public interface ISecretProvider
{
    /// <summary>
    /// Gets a secret value by key.
    /// </summary>
    Task<string?> GetSecretAsync(string key, CancellationToken cancellationToken = default);
}
