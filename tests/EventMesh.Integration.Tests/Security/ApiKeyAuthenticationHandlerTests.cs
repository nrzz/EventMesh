using EventMesh.Security;
using EventMesh.Security.Authentication;
using FluentAssertions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Encodings.Web;

namespace EventMesh.Integration.Tests.Security;

public sealed class ApiKeyAuthenticationHandlerTests
{
    [Fact]
    public async Task Handler_authenticates_valid_api_key()
    {
        const string key = "test-api-key";
        Environment.SetEnvironmentVariable("EVENTMESH_API_KEYS", key);

        try
        {
            var handler = CreateHandler();
            var context = new DefaultHttpContext();
            context.Request.Headers["X-Api-Key"] = key;

            await handler.InitializeAsync(
                new AuthenticationScheme(
                    EventMesh.Security.ApiKeyAuthenticationHandlerScheme.Name,
                    null,
                    typeof(ApiKeyAuthenticationHandler)),
                context);
            var result = await handler.AuthenticateAsync();

            result.Succeeded.Should().BeTrue();
            result.Principal?.Identity?.IsAuthenticated.Should().BeTrue();
        }
        finally
        {
            Environment.SetEnvironmentVariable("EVENTMESH_API_KEYS", null);
        }
    }

    [Fact]
    public async Task Handler_rejects_missing_api_key()
    {
        Environment.SetEnvironmentVariable("EVENTMESH_API_KEYS", "configured-key");

        try
        {
            var handler = CreateHandler();
            var context = new DefaultHttpContext();

            await handler.InitializeAsync(
                new AuthenticationScheme(
                    EventMesh.Security.ApiKeyAuthenticationHandlerScheme.Name,
                    null,
                    typeof(ApiKeyAuthenticationHandler)),
                context);
            var result = await handler.AuthenticateAsync();

            result.Succeeded.Should().BeFalse();
        }
        finally
        {
            Environment.SetEnvironmentVariable("EVENTMESH_API_KEYS", null);
        }
    }

    private static ApiKeyAuthenticationHandler CreateHandler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions();
        var provider = services.BuildServiceProvider();

        return new ApiKeyAuthenticationHandler(
            provider.GetRequiredService<IOptionsMonitor<ApiKeyAuthenticationOptions>>(),
            provider.GetRequiredService<ILoggerFactory>(),
            UrlEncoder.Default);
    }
}
