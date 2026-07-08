using EventMesh.Security.Authorization;
using FluentAssertions;
using Microsoft.AspNetCore.Authorization;
using System.Security.Claims;

namespace EventMesh.Integration.Tests.Security;

public sealed class RbacAuthorizationHandlerTests
{
    [Fact]
    public async Task Handler_succeeds_when_required_role_is_present()
    {
        var handler = new RbacAuthorizationHandler();
        var requirement = new RbacRequirement("admin");
        var user = new ClaimsPrincipal(new ClaimsIdentity(
        [
            new Claim(ClaimTypes.NameIdentifier, "user-1"),
            new Claim("role", "admin"),
        ],
        authenticationType: "test"));

        var context = new AuthorizationHandlerContext([requirement], user, resource: null);
        await handler.HandleAsync(context);

        context.HasSucceeded.Should().BeTrue();
    }

    [Fact]
    public async Task Handler_fails_when_required_role_is_missing()
    {
        var handler = new RbacAuthorizationHandler();
        var requirement = new RbacRequirement("admin");
        var user = new ClaimsPrincipal(new ClaimsIdentity(
        [
            new Claim(ClaimTypes.NameIdentifier, "user-1"),
            new Claim("role", "viewer"),
        ],
        authenticationType: "test"));

        var context = new AuthorizationHandlerContext([requirement], user, resource: null);
        await handler.HandleAsync(context);

        context.HasSucceeded.Should().BeFalse();
    }
}
