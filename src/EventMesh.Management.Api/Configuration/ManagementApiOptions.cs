namespace EventMesh.Management.Api.Configuration;

/// <summary>
/// Configuration options for the EventMesh management API.
/// </summary>
public sealed class ManagementApiOptions
{
    /// <summary>
    /// Configuration section name.
    /// </summary>
    public const string SectionName = "ManagementApi";

    /// <summary>
    /// Gets or sets valid API keys for management API access.
    /// </summary>
    public IList<string> ApiKeys { get; set; } = [];

    /// <summary>
    /// Gets or sets JWT authentication settings.
    /// </summary>
    public JwtOptions Jwt { get; set; } = new();

    /// <summary>
    /// Gets or sets the directory scanned for plugin manifests.
    /// </summary>
    public string PluginDirectory { get; set; } = "plugins";

    /// <summary>
    /// Gets or sets the observation refresh interval in seconds.
    /// </summary>
    public int RefreshIntervalSeconds { get; set; } = 5;

    /// <summary>
    /// Gets or sets allowed CORS origins for the dashboard and browser clients.
    /// Required outside Development when credentials are enabled.
    /// </summary>
    public IList<string> AllowedOrigins { get; set; } = [];

    /// <summary>
    /// Gets or sets a value indicating whether the API seeds and simulates demo broker data.
    /// Disable in production to show only real transport observations.
    /// </summary>
    public bool DemoMode { get; set; }
}

/// <summary>
/// JWT authentication placeholder settings for future OAuth2/OIDC integration.
/// </summary>
public sealed class JwtOptions
{
    /// <summary>
    /// Gets or sets the OIDC authority URL.
    /// </summary>
    public string Authority { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the expected JWT audience.
    /// </summary>
    public string Audience { get; set; } = "eventmesh-management";

    /// <summary>
    /// Gets or sets a value indicating whether authentication is required for all endpoints.
    /// </summary>
    public bool RequireAuthentication { get; set; }

    /// <summary>
    /// Gets or sets the symmetric signing key used when <see cref="Authority"/> is not configured.
    /// Required outside Development.
    /// </summary>
    public string SigningKey { get; set; } = string.Empty;
}
