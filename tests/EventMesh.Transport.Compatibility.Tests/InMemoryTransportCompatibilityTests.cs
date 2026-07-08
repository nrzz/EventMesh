using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Compatibility tests for the in-memory transport adapter.
/// </summary>
public sealed class InMemoryTransportCompatibilityTests : TransportCompatibilityTestBase
{
    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        var services = new ServiceCollection();
        services.AddInMemoryTransport(options =>
        {
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(5);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(10);
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
        if (transport is not InMemoryBrokerTransport inMemoryTransport)
        {
            throw new InvalidOperationException("Expected an in-memory transport instance.");
        }

        return inMemoryTransport.ReplayAsync(options, cancellationToken);
    }
}
