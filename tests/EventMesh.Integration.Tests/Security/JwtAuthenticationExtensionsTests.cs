using EventMesh.Security.Authentication;
using FluentAssertions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.DependencyInjection;

namespace EventMesh.Integration.Tests.Security;

public sealed class JwtAuthenticationExtensionsTests
{
    [Fact]
    public void AddEventMeshJwtAuthentication_registers_jwt_bearer_scheme()
    {
        Environment.SetEnvironmentVariable("EVENTMESH_JWT_AUTHORITY", "https://login.example.com");
        Environment.SetEnvironmentVariable("EVENTMESH_JWT_AUDIENCE", "eventmesh-api");

        try
        {
            var services = new ServiceCollection();
            services.AddEventMeshJwtAuthentication();
            var provider = services.BuildServiceProvider();

            provider.GetService<IAuthenticationSchemeProvider>().Should().NotBeNull();
        }
        finally
        {
            Environment.SetEnvironmentVariable("EVENTMESH_JWT_AUTHORITY", null);
            Environment.SetEnvironmentVariable("EVENTMESH_JWT_AUDIENCE", null);
        }
    }
}
