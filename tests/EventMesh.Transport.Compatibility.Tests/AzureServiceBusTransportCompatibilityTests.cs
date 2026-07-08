using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.AzureServiceBus;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Unit tests for the Azure Service Bus transport adapter.
/// </summary>
public sealed class AzureServiceBusTransportUnitTests
{
    private const string PlaceholderConnectionString =
        "Endpoint=sb://eventmesh-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=placeholder";

    [Fact]
    public async Task GetCapabilities_DeclaresExpectedFeatures()
    {
        await using var provider = BuildServiceProvider(PlaceholderConnectionString);
        var transport = provider.GetRequiredService<IBrokerTransport>();

        transport.Name.Should().Be("azureservicebus");
        transport.GetCapabilities().SupportsAll(
            BrokerCapabilities.Sessions
            | BrokerCapabilities.Transactions
            | BrokerCapabilities.NativeScheduling
            | BrokerCapabilities.DeadLettering
            | BrokerCapabilities.Ttl
            | BrokerCapabilities.TopologyProvisioning).Should().BeTrue();
    }

    [Fact]
    public void TopologyState_ResolvesDeadLetterAlias()
    {
        var state = new AzureServiceBusTopologyState();
        state.RegisterQueue("compat.queue", "compat.queue.dlq", requiresSession: false);

        state.TryResolveDeadLetterSource("compat.queue.dlq", out var sourceQueue).Should().BeTrue();
        sourceQueue.Should().Be("compat.queue");
    }

    [Fact]
    public void TopologyState_ResolvesSubscription()
    {
        var state = new AzureServiceBusTopologyState();
        state.RegisterSubscription("compat.subscription", "compat.events", "compat.subscription", "compat.queue");

        state.TryGetSubscription("compat.subscription", out var subscription).Should().BeTrue();
        subscription.Topic.Should().Be("compat.events");
        subscription.SubscriptionName.Should().Be("compat.subscription");
    }

    [Fact]
    public async Task Factory_AppliesConnectionStringFromSettings()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddAzureServiceBusTransport(options =>
        {
            options.ConnectionString = PlaceholderConnectionString;
            options.PrefetchCount = 5;
        });

        var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IBrokerTransportFactory>();

        factory.TransportName.Should().Be("azureservicebus");

        await using var transport = await factory.CreateTransportAsync(
            new Dictionary<string, string>
            {
                ["ConnectionString"] = PlaceholderConnectionString,
                ["PrefetchCount"] = "20",
                ["MaxConcurrentCalls"] = "4",
            },
            CancellationToken.None);

        transport.Should().BeOfType<AzureServiceBusBrokerTransport>();
        provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusTransportOptions>>()
            .Value.PrefetchCount.Should().Be(20);
    }

    private static ServiceProvider BuildServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddAzureServiceBusTransport(options =>
        {
            options.ConnectionString = connectionString;
            options.PrefetchCount = 10;
            options.MaxConcurrentCalls = 1;
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(25);
        });

        return services.BuildServiceProvider();
    }
}

/// <summary>
/// Integration compatibility tests for the Azure Service Bus transport adapter.
/// Requires <c>EVENTMESH_AZURE_SERVICEBUS_CONNECTION_STRING</c>.
/// </summary>
[Trait("Category", "Integration")]
public sealed class AzureServiceBusTransportCompatibilityTests : TransportCompatibilityTestBase
{
    private const string ConnectionStringEnvironmentVariable = "EVENTMESH_AZURE_SERVICEBUS_CONNECTION_STRING";

    protected override BrokerCapabilities RequiredCapabilities =>
        BrokerCapabilities.Sessions
        | BrokerCapabilities.Transactions
        | BrokerCapabilities.NativeScheduling
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.Ttl
        | BrokerCapabilities.TopologyProvisioning
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.Queues
        | BrokerCapabilities.RequestResponse;

    protected override TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(30);

    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                $"Set the {ConnectionStringEnvironmentVariable} environment variable to run Azure Service Bus integration tests.");
        }

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddAzureServiceBusTransport(options =>
        {
            options.ConnectionString = connectionString;
            options.PrefetchCount = 10;
            options.MaxConcurrentCalls = 1;
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(50);
            options.ReceiveWaitTime = TimeSpan.FromMilliseconds(250);
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
        if (transport is not AzureServiceBusBrokerTransport azureServiceBusTransport)
        {
            throw new InvalidOperationException("Expected an Azure Service Bus transport instance.");
        }

        return azureServiceBusTransport.ReplayAsync(options, cancellationToken);
    }
}
