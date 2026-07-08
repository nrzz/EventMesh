using Npgsql;

namespace EventMesh.Storage.PostgreSql.Schema;

/// <summary>
/// Creates EventMesh PostgreSQL storage tables and indexes.
/// </summary>
public sealed class SchemaInitializer
{
    private static readonly SemaphoreSlim InitializationLock = new(1, 1);
    private static volatile bool _initialized;

    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlStorageOptions _options;

    public SchemaInitializer(NpgsqlDataSource dataSource, Microsoft.Extensions.Options.IOptions<PostgreSqlStorageOptions> options)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
    }

    /// <summary>
    /// Ensures storage schema objects exist when auto-initialization is enabled.
    /// </summary>
    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.AutoInitializeSchema || _initialized)
        {
            return;
        }

        await InitializationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized || !_options.AutoInitializeSchema)
            {
                return;
            }

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = LoadSchemaScript();
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _initialized = true;
        }
        finally
        {
            InitializationLock.Release();
        }
    }

    private static string LoadSchemaScript()
    {
        var assembly = typeof(SchemaInitializer).Assembly;
        const string resourceName = "EventMesh.Storage.PostgreSql.Schema.OutboxSchema.sql";

        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded schema resource '{resourceName}' was not found.");

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
