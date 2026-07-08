using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Compatibility tests for the Kafka transport adapter.
/// </summary>
[Trait("Category", "Integration")]
public sealed class KafkaTransportCompatibilityTests : TransportCompatibilityTestBase, IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer = new KafkaBuilder().Build();
    private string _groupId = $"eventmesh-compat-{Guid.NewGuid():N}";

    public async Task InitializeAsync() => await _kafkaContainer.StartAsync();

    public async Task DisposeAsync() => await _kafkaContainer.DisposeAsync();

    protected override BrokerCapabilities RequiredCapabilities =>
        BrokerCapabilities.Partitions
        | BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.Replay
        | BrokerCapabilities.Ordering
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.TopologyProvisioning;

    protected override TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(30);

    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        _groupId = $"eventmesh-compat-{Guid.NewGuid():N}";

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddKafkaTransport(options =>
        {
            options.BootstrapServers = _kafkaContainer.GetBootstrapAddress();
            options.GroupId = _groupId;
            options.ConsumerPollInterval = TimeSpan.FromMilliseconds(100);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(25);
            options.DefaultPartitionCount = 1;
            options.ReplicationFactor = 1;
        });

        var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IBrokerTransportFactory>();
        return factory.CreateTransportAsync(cancellationToken: CancellationToken.None);
    }

    protected override Task<TransportCompatibilityContext> CreateContextAsync()
    {
        var topology = new TopologyDefinition
        {
            Name = "compat-topology",
            ReplaceExisting = true,
            Queues =
            [
                new QueueDefinition
                {
                    Name = TestQueue,
                    DeadLetterDestination = TestDlq,
                },
                new QueueDefinition
                {
                    Name = TestDlq,
                },
                new QueueDefinition
                {
                    Name = ReplyQueue,
                },
            ],
            Topics =
            [
                new TopicDefinition
                {
                    Name = TestTopic,
                    PartitionCount = 1,
                },
            ],
            Subscriptions =
            [
                new SubscriptionDefinition
                {
                    Name = TestSubscription,
                    Topic = TestTopic,
                    Destination = TestQueue,
                    ConsumerGroup = "compat-group",
                },
            ],
        };

        return Task.FromResult(new TransportCompatibilityContext
        {
            Topology = topology,
        });
    }

    protected override Task<long> ReplayMessagesAsync(
        IBrokerTransport transport,
        ReplayOptions options,
        CancellationToken cancellationToken)
    {
        if (transport is not KafkaBrokerTransport kafkaTransport)
        {
            throw new InvalidOperationException("Expected a Kafka transport instance.");
        }

        return kafkaTransport.ReplayAsync(options, cancellationToken);
    }
}
