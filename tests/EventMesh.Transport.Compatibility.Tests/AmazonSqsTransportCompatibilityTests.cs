using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Transport.AmazonSqs;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Compatibility tests for the Amazon SQS transport adapter using LocalStack.
/// </summary>
[Trait("Category", "Integration")]
public sealed class AmazonSqsTransportCompatibilityTests : TransportCompatibilityTestBase, IAsyncLifetime
{
    private const string LocalStackImage = "localstack/localstack:4.0";
    private const string DefaultRegion = "us-east-1";

    private IContainer? _localStackContainer;
    private string? _serviceUrl;
    private bool _useExternalEndpoint;

    protected override BrokerCapabilities RequiredCapabilities =>
        BrokerCapabilities.Fifo
        | BrokerCapabilities.DelayedDelivery
        | BrokerCapabilities.VisibilityTimeout
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.TopologyProvisioning;

    protected override TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(30);

    public async Task InitializeAsync()
    {
        _serviceUrl = ResolveExternalServiceUrl();
        if (_serviceUrl is not null)
        {
            _useExternalEndpoint = true;
            return;
        }

        _localStackContainer = new ContainerBuilder()
            .WithImage(LocalStackImage)
            .WithPortBinding(4566, true)
            .WithEnvironment("SERVICES", "sqs")
            .WithEnvironment("AWS_DEFAULT_REGION", DefaultRegion)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request =>
                request.ForPath("/_localstack/health").ForPort(4566)))
            .Build();

        await _localStackContainer.StartAsync();
        _serviceUrl = $"http://{_localStackContainer.Hostname}:{_localStackContainer.GetMappedPublicPort(4566)}";
    }

    public async Task DisposeAsync()
    {
        if (_useExternalEndpoint || _localStackContainer is null)
        {
            return;
        }

        await _localStackContainer.DisposeAsync();
    }

    protected override bool SupportsReplay => false;

    protected override Task<IBrokerTransport> CreateTransportAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddAmazonSqsTransport(options =>
        {
            options.Region = DefaultRegion;
            options.ServiceUrl = _serviceUrl;
            options.AccessKeyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID") ?? "test";
            options.SecretAccessKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY") ?? "test";
            options.VisibilityTimeoutSeconds = 30;
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(50);
            options.ReceiveWaitTimeSeconds = 1;
            options.DefaultMaxReceiveCount = 5;
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
        CancellationToken cancellationToken) =>
        throw new CapabilityNotSupportedException(BrokerCapabilities.Replay, "amazonsqs");

    private static string? ResolveExternalServiceUrl()
    {
        foreach (var variable in new[] { "AWS_ENDPOINT_URL", "LOCALSTACK_URL", "EVENTMESH_SQS_ENDPOINT" })
        {
            var value = Environment.GetEnvironmentVariable(variable);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value.TrimEnd('/');
            }
        }

        return null;
    }
}
