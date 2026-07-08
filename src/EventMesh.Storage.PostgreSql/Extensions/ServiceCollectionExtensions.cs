using EventMesh.Abstractions.Reliability;
using EventMesh.Abstractions.Scheduling;
using EventMesh.Storage.PostgreSql.Inbox;
using EventMesh.Storage.PostgreSql.Outbox;
using EventMesh.Storage.PostgreSql.Scheduling;
using EventMesh.Storage.PostgreSql.Schema;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace EventMesh.Storage.PostgreSql.Extensions;

/// <summary>
/// Dependency injection extensions for PostgreSQL-backed EventMesh storage.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers PostgreSQL-backed outbox, inbox, and scheduler storage services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <param name="configure">Optional callback to configure storage options.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddEventMeshPostgreSqlStorage(
        this IServiceCollection services,
        string connectionString,
        Action<PostgreSqlStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.AddOptions<PostgreSqlStorageOptions>()
            .Configure(options =>
            {
                options.ConnectionString = connectionString;
                configure?.Invoke(options);
            });

        services.TryAddSingleton(static sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<PostgreSqlStorageOptions>>().Value;
            return NpgsqlDataSource.Create(options.ConnectionString);
        });

        services.TryAddSingleton<SchemaInitializer>();
        services.TryAddSingleton<IOutboxStore, PostgreSqlOutboxStore>();
        services.TryAddSingleton<IInboxStore, PostgreSqlInboxStore>();
        services.TryAddSingleton<IMessageScheduler, PostgreSqlMessageScheduler>();

        return services;
    }
}
