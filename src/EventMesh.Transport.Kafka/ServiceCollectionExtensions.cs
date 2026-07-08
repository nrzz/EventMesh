using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Transport.Kafka;

/// <summary>
/// Dependency injection extensions for the Kafka transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Kafka broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddKafkaTransport(
        this IServiceCollection services,
        Action<KafkaTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<KafkaTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<KafkaTopologyManager>();
        services.TryAddSingleton<IBrokerTransportFactory, KafkaTransportFactory>();
        services.TryAddSingleton<IBrokerTransport, KafkaBrokerTransport>();

        return services;
    }
}
