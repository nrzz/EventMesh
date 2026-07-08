using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.GooglePubSub;
using Google.Api.Gax;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.PubSub;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Compatibility tests for the Google Pub/Sub transport adapter.
/// </summary>
[Trait("Category", "Integration")]
public sealed class GooglePubSubTransportCompatibilityTests : TransportCompatibilityTestBase, IAsyncLifetime
{
    private readonly PubSubContainer _pubSubContainer = new PubSubBuilder().Build();
    private string? _originalEmulatorHost;

    public async Task InitializeAsync()
    {
        await _pubSubContainer.StartAsync();
        _originalEmulatorHost = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST");

        var emulatorEndpoint = new Uri(_pubSubContainer.GetEmulatorEndpoint());
        Environment.SetEnvironmentVariable(
            "PUBSUB_EMULATOR_HOST",
            $"{emulatorEndpoint.Host}:{emulatorEndpoint.Port}");
    }

    public async Task DisposeAsync()
    {
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", _originalEmulatorHost);
        await _pubSubContainer.DisposeAsync();
    }

    protected override BrokerCapabilities RequiredCapabilities =>
        BrokerCapabilities.Ordering
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.TopologyProvisioning;

    protected override TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(30);

    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddGooglePubSubTransport(options =>
        {
            options.ProjectId = "eventmesh-compat";
            options.EmulatorDetection = EmulatorDetection.EmulatorOnly;
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(50);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(25);
            options.EnableMessageOrdering = true;
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
        if (transport is not GooglePubSubBrokerTransport googlePubSubTransport)
        {
            throw new InvalidOperationException("Expected a Google Pub/Sub transport instance.");
        }

        return googlePubSubTransport.ReplayAsync(options, cancellationToken);
    }
}
