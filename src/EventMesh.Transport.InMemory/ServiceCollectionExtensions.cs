using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Transport.InMemory;

/// <summary>
/// Dependency injection extensions for the in-memory transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the in-memory broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddInMemoryTransport(
        this IServiceCollection services,
        Action<InMemoryTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<InMemoryTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<InMemoryMessageStore>();
        services.TryAddSingleton<InMemoryBrokerState>();
        services.TryAddSingleton<InMemoryTransportFactory>();
        services.TryAddSingleton<IBrokerTransportFactory>(sp => sp.GetRequiredService<InMemoryTransportFactory>());
        services.TryAddSingleton<IBrokerTransport, InMemoryBrokerTransport>();

        return services;
    }
}
