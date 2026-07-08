using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.Kafka;

/// <summary>
/// Creates <see cref="KafkaBrokerTransport"/> instances.
/// </summary>
public sealed class KafkaTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public KafkaTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "kafka";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<IOptions<KafkaTransportOptions>>().Value;
            ApplySettings(options, settings);
        }

        IBrokerTransport transport = ActivatorUtilities.CreateInstance<KafkaBrokerTransport>(_serviceProvider);
        return Task.FromResult(transport);
    }

    private static void ApplySettings(KafkaTransportOptions options, IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("BootstrapServers", out var bootstrapServers))
        {
            options.BootstrapServers = bootstrapServers;
        }

        if (settings.TryGetValue("GroupId", out var groupId))
        {
            options.GroupId = groupId;
        }

        if (settings.TryGetValue("Acks", out var acks)
            && Enum.TryParse<Confluent.Kafka.Acks>(acks, ignoreCase: true, out var parsedAcks))
        {
            options.Acks = parsedAcks;
        }

        if (settings.TryGetValue("CompressionType", out var compressionType)
            && Enum.TryParse<Confluent.Kafka.CompressionType>(compressionType, ignoreCase: true, out var parsedCompression))
        {
            options.CompressionType = parsedCompression;
        }

        if (settings.TryGetValue("DefaultPartitionCount", out var defaultPartitionCount)
            && int.TryParse(defaultPartitionCount, out var partitionCount))
        {
            options.DefaultPartitionCount = partitionCount;
        }

        if (settings.TryGetValue("ReplicationFactor", out var replicationFactor)
            && short.TryParse(replicationFactor, out var parsedReplicationFactor))
        {
            options.ReplicationFactor = parsedReplicationFactor;
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
    }
}
