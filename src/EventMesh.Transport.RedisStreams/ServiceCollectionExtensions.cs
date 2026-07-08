using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventMesh.Transport.RedisStreams;

/// <summary>
/// Dependency injection extensions for the Redis Streams transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Redis Streams broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddRedisStreamsTransport(
        this IServiceCollection services,
        Action<RedisStreamsTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<RedisStreamsTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<IConnectionMultiplexer>(static serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<RedisStreamsTransportOptions>>().Value;
            return ConnectionMultiplexer.Connect(options.ConnectionString);
        });

        services.TryAddTransient<RedisStreamsTopologyManager>();
        services.TryAddSingleton<IBrokerTransportFactory, RedisStreamsTransportFactory>();
        services.TryAddTransient<IBrokerTransport, RedisStreamsBrokerTransport>();

        return services;
    }
}
