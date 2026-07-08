using EventMesh.Security.Authentication;
using EventMesh.Security.Authorization;
using EventMesh.Security.Secrets;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Security;

/// <summary>
/// Dependency injection extensions for EventMesh security services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers EventMesh security services including secret providers and RBAC.
    /// </summary>
    public static IServiceCollection AddEventMeshSecurity(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<EnvironmentSecretProvider>();
        services.TryAddSingleton<VaultSecretProvider>();
        services.TryAddSingleton<ISecretProvider>(sp => sp.GetRequiredService<EnvironmentSecretProvider>());

        services.AddSingleton<IAuthorizationHandler, RbacAuthorizationHandler>();

        services.AddAuthentication()
            .AddScheme<ApiKeyAuthenticationOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationHandlerScheme.Name,
                _ => { });

        return services;
    }
}

/// <summary>
/// Authentication scheme name for API key authentication.
/// </summary>
public static class ApiKeyAuthenticationHandlerScheme
{
    public const string Name = "ApiKey";
}
