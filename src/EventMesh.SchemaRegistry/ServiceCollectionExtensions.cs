using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.SchemaRegistry;

/// <summary>
/// Dependency injection extensions for the schema registry.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the in-memory schema registry and validators.
    /// </summary>
    public static IServiceCollection AddEventMeshSchemaRegistry(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<IJsonSchemaValidator, JsonSchemaValidator>();
        services.TryAddSingleton<IAvroSchemaValidator, AvroSchemaValidator>();
        services.TryAddSingleton<ISchemaRegistry, InMemorySchemaRegistry>();

        return services;
    }
}
