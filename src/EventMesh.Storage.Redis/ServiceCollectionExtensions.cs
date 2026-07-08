using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace EventMesh.Storage.Redis;

/// <summary>
/// DI extensions for Redis storage components.
/// </summary>
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEventMeshRedisStorage(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.AddSingleton<IConnectionMultiplexer>(_ =>
            ConnectionMultiplexer.Connect(connectionString));

        services.AddSingleton<RedisDistributedLock>();
        return services;
    }
}
