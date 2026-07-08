using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.RabbitMq;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Compatibility tests for the RabbitMQ transport adapter.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RabbitMqTransportCompatibilityTests : TransportCompatibilityTestBase, IAsyncLifetime
{
    private readonly RabbitMqContainer _rabbitMqContainer = new RabbitMqBuilder()
        .WithImage("rabbitmq:3.13-management-alpine")
        .Build();

    protected override TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(15);

    public async Task InitializeAsync()
    {
        await _rabbitMqContainer.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _rabbitMqContainer.DisposeAsync();
    }

    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        var connectionString = new Uri(_rabbitMqContainer.GetConnectionString());

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddRabbitMqTransport(options =>
        {
            options.HostName = connectionString.Host;
            options.Port = connectionString.Port;
            options.UserName = GetUserInfoValue(connectionString, user: true) ?? "guest";
            options.Password = GetUserInfoValue(connectionString, user: false) ?? "guest";
            options.VirtualHost = ResolveVirtualHost(connectionString);
            options.PrefetchCount = 10;
            options.PublisherConfirmsEnabled = true;
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(25);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(50);
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
                    Type = "topic",
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
        if (transport is not RabbitMqBrokerTransport rabbitMqTransport)
        {
            throw new InvalidOperationException("Expected a RabbitMQ transport instance.");
        }

        return rabbitMqTransport.ReplayAsync(options, cancellationToken);
    }

    private static string? GetUserInfoValue(Uri connectionString, bool user)
    {
        if (string.IsNullOrEmpty(connectionString.UserInfo))
        {
            return null;
        }

        var parts = connectionString.UserInfo.Split(':', 2);
        return user ? Uri.UnescapeDataString(parts[0]) : Uri.UnescapeDataString(parts.Length > 1 ? parts[1] : string.Empty);
    }

    private static string ResolveVirtualHost(Uri connectionString)
    {
        var path = connectionString.AbsolutePath;
        if (string.IsNullOrEmpty(path) || path == "/")
        {
            return "/";
        }

        return Uri.UnescapeDataString(path.TrimStart('/'));
    }
}
