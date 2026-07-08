using System.Text;
using EventMesh.Abstractions.Configuration;
using EventMesh.Core;
using EventMesh.Management.Api.Auth;
using EventMesh.Management.Api.Configuration;
using EventMesh.Management.Api.Hubs;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<EventMeshOptions>(builder.Configuration.GetSection("EventMesh"));
builder.Services.Configure<ManagementApiOptions>(builder.Configuration.GetSection(ManagementApiOptions.SectionName));

builder.Services.AddEventMesh(mesh => mesh.Configure(options =>
{
    builder.Configuration.GetSection("EventMesh").Bind(options);
}));

builder.Services.AddSingleton<IMeshObservationService, MeshObservationService>();
builder.Services.AddHostedService<ObservationRefreshHostedService>();

builder.Services.AddControllers();
builder.Services.AddSignalR();
builder.Services.AddOpenApi();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .AllowAnyHeader()
            .AllowAnyMethod()
            .SetIsOriginAllowed(_ => true)
            .AllowCredentials();
    });
});

var managementOptions = builder.Configuration
    .GetSection(ManagementApiOptions.SectionName)
    .Get<ManagementApiOptions>() ?? new ManagementApiOptions();

var authenticationBuilder = builder.Services.AddAuthentication(options =>
{
    options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
    options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
})
.AddScheme<Microsoft.AspNetCore.Authentication.AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
    ApiKeyAuthenticationDefaults.Scheme,
    _ => { });

if (!string.IsNullOrWhiteSpace(managementOptions.Jwt.Authority))
{
    authenticationBuilder.AddJwtBearer(options =>
    {
        options.Authority = managementOptions.Jwt.Authority;
        options.Audience = managementOptions.Jwt.Audience;
        options.RequireHttpsMetadata = !builder.Environment.IsDevelopment();
    });
}
else
{
    authenticationBuilder.AddJwtBearer(options =>
    {
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidateAudience = true,
            ValidateLifetime = true,
            ValidateIssuerSigningKey = true,
            ValidIssuer = "eventmesh-management",
            ValidAudience = managementOptions.Jwt.Audience,
            IssuerSigningKey = new SymmetricSecurityKey(
                Encoding.UTF8.GetBytes("eventmesh-dev-signing-key-change-in-production-32b")),
        };
    });
}

builder.Services.AddAuthorization(options =>
{
    if (managementOptions.Jwt.RequireAuthentication)
    {
        options.FallbackPolicy = new Microsoft.AspNetCore.Authorization.AuthorizationPolicyBuilder(
                JwtBearerDefaults.AuthenticationScheme,
                ApiKeyAuthenticationDefaults.Scheme)
            .RequireAuthenticatedUser()
            .Build();
    }
});

builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy("Management API is running."));

var otlpEndpoint = builder.Configuration["OpenTelemetry:OtlpEndpoint"];
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("EventMesh.Management.Api"))
    .WithMetrics(metrics =>
    {
        metrics
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddPrometheusExporter();

        if (!string.IsNullOrWhiteSpace(otlpEndpoint))
        {
            metrics.AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint));
        }
    })
    .WithTracing(tracing =>
    {
        tracing
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation();

        if (!string.IsNullOrWhiteSpace(otlpEndpoint))
        {
            tracing.AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint));
        }
    });

var app = builder.Build();

app.UseCors();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();
app.MapHub<EventMeshHub>("/hubs/eventmesh");
app.MapHealthChecks("/health");
app.MapPrometheusScrapingEndpoint("/metrics");

app.Run();
