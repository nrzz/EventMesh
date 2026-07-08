using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.Nats;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.Nats;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Compatibility tests for the NATS JetStream transport adapter.
/// </summary>
[Trait("Category", "Integration")]
public sealed class NatsTransportCompatibilityTests : TransportCompatibilityTestBase, IAsyncLifetime
{
    private readonly NatsContainer _natsContainer = new NatsBuilder()
        .WithImage("nats:2.10-alpine")
        .Build();
    private string _consumerPrefix = $"eventmesh-compat-{Guid.NewGuid():N}";

    public async Task InitializeAsync() => await _natsContainer.StartAsync();

    public async Task DisposeAsync() => await _natsContainer.DisposeAsync();

    protected override BrokerCapabilities RequiredCapabilities =>
        BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.TopologyProvisioning;

    protected override TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(30);

    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        _consumerPrefix = $"eventmesh-compat-{Guid.NewGuid():N}";

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddNatsTransport(options =>
        {
            options.Url = _natsContainer.GetConnectionString();
            options.ConsumerPrefix = _consumerPrefix;
            options.FetchTimeout = TimeSpan.FromMilliseconds(100);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(25);
            options.DefaultMaxDeliveryAttempts = 5;
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
        if (transport is not NatsJetStreamBrokerTransport natsTransport)
        {
            throw new InvalidOperationException("Expected a NATS transport instance.");
        }

        return natsTransport.ReplayAsync(options, cancellationToken);
    }
}
