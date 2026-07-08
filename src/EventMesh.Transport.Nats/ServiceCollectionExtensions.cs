using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Transport.Nats;

/// <summary>
/// Dependency injection extensions for the NATS JetStream transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the NATS JetStream broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddNatsTransport(
        this IServiceCollection services,
        Action<NatsTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<NatsTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<NatsConnectionManager>();
        services.TryAddSingleton<NatsTopologyManager>();
        services.TryAddSingleton<IBrokerTransportFactory, NatsTransportFactory>();
        services.TryAddSingleton<IBrokerTransport, NatsJetStreamBrokerTransport>();

        return services;
    }
}
