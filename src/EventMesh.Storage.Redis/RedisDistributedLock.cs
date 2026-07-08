using StackExchange.Redis;

namespace EventMesh.Storage.Redis;

/// <summary>
/// Redis-based distributed lock for consumer coordination.
/// </summary>
public sealed class RedisDistributedLock
{
    private readonly IDatabase _database;
    private readonly string _keyPrefix;

    public RedisDistributedLock(IConnectionMultiplexer connection, string keyPrefix = "eventmesh:lock:")
    {
        ArgumentNullException.ThrowIfNull(connection);
        _database = connection.GetDatabase();
        _keyPrefix = keyPrefix;
    }

    public async Task<bool> TryAcquireAsync(string resource, TimeSpan expiry, CancellationToken cancellationToken = default)
    {
        var token = Guid.NewGuid().ToString("N");
        return await _database.StringSetAsync(
            _keyPrefix + resource,
            token,
            expiry,
            When.NotExists).ConfigureAwait(false);
    }

    public async Task ReleaseAsync(string resource, CancellationToken cancellationToken = default)
    {
        await _database.KeyDeleteAsync(_keyPrefix + resource).ConfigureAwait(false);
    }
}
