using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.RedisStreams;

/// <summary>
/// Creates configured <see cref="RedisStreamsBrokerTransport"/> instances.
/// </summary>
public sealed class RedisStreamsTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public RedisStreamsTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "redisstreams";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<IOptions<RedisStreamsTransportOptions>>().Value;
            ApplySettings(options, settings);
        }

        IBrokerTransport transport = ActivatorUtilities.CreateInstance<RedisStreamsBrokerTransport>(_serviceProvider);
        return Task.FromResult(transport);
    }

    private static void ApplySettings(RedisStreamsTransportOptions options, IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("ConnectionString", out var connectionString))
        {
            options.ConnectionString = connectionString;
        }

        if (settings.TryGetValue("StreamPrefix", out var streamPrefix))
        {
            options.StreamPrefix = streamPrefix;
        }

        if (settings.TryGetValue("ConsumerName", out var consumerName))
        {
            options.ConsumerName = consumerName;
        }

        if (settings.TryGetValue("ReceivePollInterval", out var receivePollInterval)
            && TimeSpan.TryParse(receivePollInterval, out var pollInterval))
        {
            options.ReceivePollInterval = pollInterval;
        }

        if (settings.TryGetValue("ReceiveBlockDuration", out var receiveBlockDuration)
            && TimeSpan.TryParse(receiveBlockDuration, out var blockDuration))
        {
            options.ReceiveBlockDuration = blockDuration;
        }

        if (settings.TryGetValue("PendingClaimMinIdle", out var pendingClaimMinIdle)
            && TimeSpan.TryParse(pendingClaimMinIdle, out var minIdle))
        {
            options.PendingClaimMinIdle = minIdle;
        }

        if (settings.TryGetValue("DelayCheckInterval", out var delayCheckInterval)
            && TimeSpan.TryParse(delayCheckInterval, out var delayInterval))
        {
            options.DelayCheckInterval = delayInterval;
        }

        if (settings.TryGetValue("DefaultDeadLetterSuffix", out var deadLetterSuffix))
        {
            options.DefaultDeadLetterSuffix = deadLetterSuffix;
        }

        if (settings.TryGetValue("DefaultMaxDeliveryAttempts", out var maxDeliveryAttempts)
            && int.TryParse(maxDeliveryAttempts, out var attempts))
        {
            options.DefaultMaxDeliveryAttempts = attempts;
        }

        if (settings.TryGetValue("MaxPriority", out var maxPriority)
            && int.TryParse(maxPriority, out var priority))
        {
            options.MaxPriority = priority;
        }

        if (settings.TryGetValue("DelayedMessagesKey", out var delayedMessagesKey))
        {
            options.DelayedMessagesKey = delayedMessagesKey;
        }
    }
}
