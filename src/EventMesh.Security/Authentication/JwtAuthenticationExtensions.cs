using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;

namespace EventMesh.Security.Authentication;

/// <summary>
/// JWT authentication registration extensions.
/// </summary>
public static class JwtAuthenticationExtensions
{
    /// <summary>
    /// Adds JWT bearer authentication using environment-driven configuration.
    /// </summary>
    public static IServiceCollection AddEventMeshJwtAuthentication(
        this IServiceCollection services,
        Action<JwtBearerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            })
            .AddJwtBearer(options =>
            {
                var authority = Environment.GetEnvironmentVariable("EVENTMESH_JWT_AUTHORITY");
                var audience = Environment.GetEnvironmentVariable("EVENTMESH_JWT_AUDIENCE");

                if (!string.IsNullOrWhiteSpace(authority))
                {
                    options.Authority = authority;
                    options.RequireHttpsMetadata = authority.StartsWith("https://", StringComparison.OrdinalIgnoreCase);
                }

                if (!string.IsNullOrWhiteSpace(audience))
                {
                    options.Audience = audience;
                }

                options.TokenValidationParameters.ValidateIssuer = !string.IsNullOrWhiteSpace(authority);
                options.TokenValidationParameters.ValidateAudience = !string.IsNullOrWhiteSpace(audience);
                configure?.Invoke(options);
            });

        return services;
    }
}
