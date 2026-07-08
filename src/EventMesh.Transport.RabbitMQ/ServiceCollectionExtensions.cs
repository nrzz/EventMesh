using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// Dependency injection extensions for the RabbitMQ transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the RabbitMQ broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddRabbitMqTransport(
        this IServiceCollection services,
        Action<RabbitMqTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<RabbitMqTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<RabbitMqConnectionManager>();
        services.TryAddSingleton<RabbitMqChannelPool>();
        services.TryAddSingleton<IBrokerTransportFactory, RabbitMqTransportFactory>();
        services.TryAddSingleton<IBrokerTransport, RabbitMqBrokerTransport>();

        return services;
    }
}
