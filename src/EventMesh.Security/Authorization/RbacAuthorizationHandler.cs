using Microsoft.AspNetCore.Authorization;

namespace EventMesh.Security.Authorization;

/// <summary>
/// Role-based access control requirement.
/// </summary>
public sealed class RbacRequirement : IAuthorizationRequirement
{
    public RbacRequirement(params string[] roles)
    {
        Roles = roles;
    }

    public IReadOnlyList<string> Roles { get; }
}

/// <summary>
/// Enforces RBAC role requirements on authorized requests.
/// </summary>
public sealed class RbacAuthorizationHandler : AuthorizationHandler<RbacRequirement>
{
    /// <inheritdoc />
    protected override Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        RbacRequirement requirement)
    {
        if (context.User.Identity?.IsAuthenticated != true)
        {
            return Task.CompletedTask;
        }

        var roleClaims = context.User.FindAll("role")
            .Concat(context.User.FindAll(System.Security.Claims.ClaimTypes.Role))
            .Select(claim => claim.Value);

        if (requirement.Roles.Any(role => roleClaims.Contains(role, StringComparer.OrdinalIgnoreCase)))
        {
            context.Succeed(requirement);
        }

        return Task.CompletedTask;
    }
}
