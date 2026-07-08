using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.RedisStreams;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.Redis;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Compatibility tests for the Redis Streams transport adapter.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RedisStreamsTransportCompatibilityTests : TransportCompatibilityTestBase, IAsyncLifetime
{
    private readonly RedisContainer _redis = new RedisBuilder().Build();
    private string _connectionString = string.Empty;

    protected override TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(10);

    protected override BrokerCapabilities RequiredCapabilities =>
        BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.PendingMessages
        | BrokerCapabilities.DelayedDelivery
        | BrokerCapabilities.Priority
        | BrokerCapabilities.RequestResponse
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.Queues;

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();
        _connectionString = _redis.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        await _redis.DisposeAsync();
    }

    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddRedisStreamsTransport(options =>
        {
            options.ConnectionString = _connectionString;
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(10);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(10);
            options.ReceiveBlockDuration = TimeSpan.FromMilliseconds(50);
            options.PendingClaimMinIdle = TimeSpan.FromMilliseconds(50);
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
        if (transport is not RedisStreamsBrokerTransport redisTransport)
        {
            throw new InvalidOperationException("Expected a Redis Streams transport instance.");
        }

        return redisTransport.ReplayAsync(options, cancellationToken);
    }
}
