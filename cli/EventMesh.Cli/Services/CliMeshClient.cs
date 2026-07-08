using System.Diagnostics;
using System.Net.Http.Json;
using System.Reflection;
using System.Text;
using System.Text.Json;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Plugins;
using EventMesh.Abstractions.Transport;
using EventMesh.Core;
using EventMesh.Core.Configuration;
using EventMesh.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventMesh.Cli.Services;

/// <summary>
/// Connects to the management API or uses the local in-memory transport.
/// </summary>
public sealed class CliMeshClient : IAsyncDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
    };

    private readonly Uri? _apiBaseUri;
    private readonly HttpClient? _httpClient;
    private readonly IHost? _localHost;
    private readonly IServiceProvider? _localServices;
    private readonly bool _ownsHttpClient;

    private CliMeshClient(Uri? apiBaseUri, HttpClient? httpClient, IHost? localHost, bool ownsHttpClient)
    {
        _apiBaseUri = apiBaseUri;
        _httpClient = httpClient;
        _localHost = localHost;
        _localServices = localHost?.Services;
        _ownsHttpClient = ownsHttpClient;
    }

    public bool UsesManagementApi => _apiBaseUri is not null;

    public static async Task<CliMeshClient> CreateAsync(string? apiUrl, CancellationToken cancellationToken = default)
    {
        if (!string.IsNullOrWhiteSpace(apiUrl))
        {
            var baseUri = NormalizeApiUri(apiUrl);
            var httpClient = new HttpClient
            {
                BaseAddress = baseUri,
                Timeout = TimeSpan.FromSeconds(30),
            };

            return new CliMeshClient(baseUri, httpClient, localHost: null, ownsHttpClient: true);
        }

        var (host, factoryReference) = CreateLocalHost();
        factoryReference.Factory = Microsoft.Extensions.DependencyInjection.ActivatorUtilities
            .CreateInstance<InMemoryTransportFactory>(host.Services);
        await host.StartAsync(cancellationToken);
        return new CliMeshClient(apiBaseUri: null, httpClient: null, localHost: host, ownsHttpClient: false);
    }

    public async Task PublishAsync(
        string destination,
        string body,
        string? routingKey = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentNullException.ThrowIfNull(body);

        if (UsesManagementApi)
        {
            var request = new PublishRequest
            {
                Destination = destination,
                Body = body,
                RoutingKey = routingKey,
            };

            using var response = await _httpClient!
                .PostAsJsonAsync("publish", request, JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            response.EnsureSuccessStatusCode();
            return;
        }

        await EnsureLocalTopologyAsync(destination, cancellationToken).ConfigureAwait(false);

        var messageBus = LocalServices.GetRequiredService<IMessageBus>();
        await messageBus.PublishAsync(
            body,
            new PublishOptions
            {
                Topic = destination,
                RoutingKey = routingKey,
                MessageType = "eventmesh.cli.message",
                ContentType = "application/json",
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async Task SubscribeAsync(
        string destination,
        int? maxMessages,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);

        if (UsesManagementApi)
        {
            var requestUri = new StringBuilder("subscribe?destination=")
                .Append(Uri.EscapeDataString(destination));

            if (maxMessages is not null)
            {
                requestUri.Append("&maxMessages=").Append(maxMessages.Value);
            }

            using var response = await _httpClient!
                .GetAsync(requestUri.ToString(), HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);

            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var reader = new StreamReader(stream);

            while (!cancellationToken.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                if (line is null)
                {
                    break;
                }

                if (!string.IsNullOrWhiteSpace(line))
                {
                    Console.WriteLine(line);
                }
            }

            return;
        }

        await EnsureLocalTopologyAsync(destination, cancellationToken).ConfigureAwait(false);

        var messageBus = LocalServices.GetRequiredService<IMessageBus>();
        var received = 0;
        using var stopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var consumer = await messageBus.SubscribeAsync<string>(
            (message, _) =>
            {
                Console.WriteLine(message);
                received++;

                if (maxMessages is not null && received >= maxMessages.Value)
                {
                    stopCts.Cancel();
                }

                return Task.CompletedTask;
            },
            new SubscribeOptions
            {
                Topic = destination,
            },
            stopCts.Token).ConfigureAwait(false);

        await consumer.StartAsync(stopCts.Token).ConfigureAwait(false);

        try
        {
            await Task.Delay(Timeout.Infinite, stopCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stopCts.IsCancellationRequested)
        {
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    public async Task<long> ReplayAsync(
        string source,
        ReplayOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        options ??= new ReplayOptions();
        options.Source ??= source;

        if (UsesManagementApi)
        {
            var request = new ReplayRequest
            {
                Source = options.Source,
                Destination = options.Destination,
                From = options.From,
                To = options.To,
                FromOffset = options.FromOffset,
                ToOffset = options.ToOffset,
                MaxMessages = options.MaxMessages,
            };

            using var response = await _httpClient!
                .PostAsJsonAsync("replay", request, JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            response.EnsureSuccessStatusCode();
            var result = await response.Content.ReadFromJsonAsync<ReplayResponse>(JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            return result?.Replayed ?? 0;
        }

        var transport = await GetLocalTransportAsync(cancellationToken).ConfigureAwait(false);
        if (transport is not InMemoryBrokerTransport inMemoryTransport)
        {
            throw new InvalidOperationException("Replay requires the in-memory transport in local mode.");
        }

        return await inMemoryTransport.ReplayAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> ListQueuesAsync(CancellationToken cancellationToken = default)
    {
        if (UsesManagementApi)
        {
            var names = await _httpClient!
                .GetFromJsonAsync<List<string>>("queues", JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            return names ?? [];
        }

        var brokerState = LocalServices.GetRequiredService<InMemoryBrokerState>();
        return brokerState.GetQueueNames();
    }

    public async Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        if (UsesManagementApi)
        {
            var names = await _httpClient!
                .GetFromJsonAsync<List<string>>("topics", JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            return names ?? [];
        }

        var brokerState = LocalServices.GetRequiredService<InMemoryBrokerState>();
        return brokerState.GetTopicNames();
    }

    public async Task<IReadOnlyList<PluginInfo>> ListPluginsAsync(
        string? pluginsDirectory,
        CancellationToken cancellationToken = default)
    {
        if (UsesManagementApi)
        {
            var plugins = await _httpClient!
                .GetFromJsonAsync<List<PluginInfo>>("plugins", JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            return plugins ?? [];
        }

        var pluginsFromDi = LocalServices
            .GetServices<IEventMeshPlugin>()
            .Select(plugin => new PluginInfo
            {
                Name = plugin.Name,
                Version = plugin.Version.ToString(),
                Source = "registered",
            })
            .ToList();

        if (string.IsNullOrWhiteSpace(pluginsDirectory))
        {
            return pluginsFromDi;
        }

        var discovered = DiscoverPluginsFromDirectory(pluginsDirectory);
        return pluginsFromDi
            .Concat(discovered)
            .GroupBy(plugin => plugin.Name, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .OrderBy(plugin => plugin.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task<HealthReport> GetHealthAsync(CancellationToken cancellationToken = default)
    {
        if (UsesManagementApi)
        {
            var report = await _httpClient!
                .GetFromJsonAsync<HealthReport>("health", JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            return report ?? new HealthReport { Status = "Unknown" };
        }

        var transport = await GetLocalTransportAsync(cancellationToken).ConfigureAwait(false);
        return new HealthReport
        {
            Status = "Healthy",
            Transport = transport.Name,
            Mode = "local",
        };
    }

    public async Task<BenchmarkResult> RunBenchmarkAsync(
        int messageCount,
        int concurrency,
        string destination,
        CancellationToken cancellationToken = default)
    {
        if (messageCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(messageCount), "Message count must be greater than zero.");
        }

        if (concurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(concurrency), "Concurrency must be greater than zero.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(destination);

        if (UsesManagementApi)
        {
            var request = new BenchmarkRequest
            {
                MessageCount = messageCount,
                Concurrency = concurrency,
                Destination = destination,
            };

            using var response = await _httpClient!
                .PostAsJsonAsync("benchmark", request, JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            response.EnsureSuccessStatusCode();
            var result = await response.Content.ReadFromJsonAsync<BenchmarkResult>(JsonOptions, cancellationToken)
                .ConfigureAwait(false);

            return result ?? new BenchmarkResult();
        }

        await EnsureLocalTopologyAsync(destination, cancellationToken).ConfigureAwait(false);

        var messageBus = LocalServices.GetRequiredService<IMessageBus>();
        var payload = "eventmesh-benchmark-payload";
        var stopwatch = Stopwatch.StartNew();
        var published = 0L;
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = concurrency,
            CancellationToken = cancellationToken,
        };

        await Parallel.ForEachAsync(
            Enumerable.Range(0, messageCount),
            parallelOptions,
            async (_, token) =>
            {
                await messageBus.PublishAsync(
                    payload,
                    new PublishOptions
                    {
                        Topic = destination,
                        MessageType = "eventmesh.cli.benchmark",
                    },
                    token).ConfigureAwait(false);

                Interlocked.Increment(ref published);
            }).ConfigureAwait(false);

        stopwatch.Stop();

        return new BenchmarkResult
        {
            MessageCount = messageCount,
            Concurrency = concurrency,
            Destination = destination,
            ElapsedMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
            MessagesPerSecond = messageCount / Math.Max(stopwatch.Elapsed.TotalSeconds, 0.001),
        };
    }

    private IServiceProvider LocalServices =>
        _localServices ?? throw new InvalidOperationException("Local services are not available in management API mode.");

    public async ValueTask DisposeAsync()
    {
        if (_localHost is not null)
        {
            await _localHost.StopAsync().ConfigureAwait(false);
            _localHost.Dispose();
        }

        if (_httpClient is not null && _ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }

    private static (IHost Host, TransportFactoryReference Reference) CreateLocalHost()
    {
        var factoryReference = new TransportFactoryReference();

        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Logging.ClearProviders();
        hostBuilder.Logging.SetMinimumLevel(LogLevel.Warning);

        hostBuilder.Services.AddSingleton(factoryReference);
        hostBuilder.Services.AddInMemoryTransport(options =>
        {
            options.ReceivePollInterval = TimeSpan.FromMilliseconds(5);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(25);
        });

        hostBuilder.Services.AddEventMesh(mesh =>
        {
            mesh.UseTransport(new ReferencingTransportFactory(factoryReference));
        });

        return (hostBuilder.Build(), factoryReference);
    }

    private static Uri NormalizeApiUri(string apiUrl)
    {
        var trimmed = apiUrl.Trim();
        if (!trimmed.EndsWith('/'))
        {
            trimmed += "/";
        }

        return new Uri(trimmed, UriKind.Absolute);
    }

    private async Task EnsureLocalTopologyAsync(string destination, CancellationToken cancellationToken)
    {
        var transport = await GetLocalTransportAsync(cancellationToken).ConfigureAwait(false);
        var brokerState = LocalServices.GetRequiredService<InMemoryBrokerState>();

        var queueNames = brokerState.GetQueueNames();
        var topicNames = brokerState.GetTopicNames();

        if (queueNames.Contains(destination, StringComparer.OrdinalIgnoreCase)
            || topicNames.Contains(destination, StringComparer.OrdinalIgnoreCase))
        {
            return;
        }

        var topology = new TopologyDefinition
        {
            Name = "cli-auto",
            ReplaceExisting = false,
            Queues =
            [
                new QueueDefinition
                {
                    Name = destination,
                },
            ],
        };

        await transport.CreateTopologyAsync(topology, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IBrokerTransport> GetLocalTransportAsync(CancellationToken cancellationToken)
    {
        var factory = LocalServices.GetRequiredService<IBrokerTransportFactory>();
        return await factory.CreateTransportAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private static IEnumerable<PluginInfo> DiscoverPluginsFromDirectory(string pluginsDirectory)
    {
        if (!Directory.Exists(pluginsDirectory))
        {
            return [];
        }

        var results = new List<PluginInfo>();
        foreach (var assemblyPath in Directory.EnumerateFiles(pluginsDirectory, "*.dll"))
        {
            try
            {
                var assembly = Assembly.LoadFrom(assemblyPath);
                foreach (var pluginType in assembly.GetTypes()
                             .Where(type => typeof(IEventMeshPlugin).IsAssignableFrom(type)
                                 && type is { IsAbstract: false, IsInterface: false }))
                {
                    if (Activator.CreateInstance(pluginType) is not IEventMeshPlugin plugin)
                    {
                        continue;
                    }

                    results.Add(new PluginInfo
                    {
                        Name = plugin.Name,
                        Version = plugin.Version.ToString(),
                        Source = Path.GetFileName(assemblyPath),
                    });
                }
            }
            catch (Exception ex)
            {
                results.Add(new PluginInfo
                {
                    Name = Path.GetFileNameWithoutExtension(assemblyPath),
                    Version = "unknown",
                    Source = Path.GetFileName(assemblyPath),
                    Error = ex.Message,
                });
            }
        }

        return results;
    }

    private sealed class PublishRequest
    {
        public required string Destination { get; init; }

        public required string Body { get; init; }

        public string? RoutingKey { get; init; }
    }

    private sealed class ReplayRequest
    {
        public string? Source { get; init; }

        public string? Destination { get; init; }

        public DateTimeOffset? From { get; init; }

        public DateTimeOffset? To { get; init; }

        public long? FromOffset { get; init; }

        public long? ToOffset { get; init; }

        public int? MaxMessages { get; init; }
    }

    private sealed class ReplayResponse
    {
        public long Replayed { get; init; }
    }

    private sealed class BenchmarkRequest
    {
        public int MessageCount { get; init; }

        public int Concurrency { get; init; }

        public required string Destination { get; init; }
    }
}

public sealed class HealthReport
{
    public required string Status { get; init; }

    public string? Transport { get; init; }

    public string? Mode { get; init; }
}

public sealed class PluginInfo
{
    public required string Name { get; init; }

    public required string Version { get; init; }

    public string? Source { get; init; }

    public string? Error { get; init; }
}

public sealed class BenchmarkResult
{
    public int MessageCount { get; init; }

    public int Concurrency { get; init; }

    public string? Destination { get; init; }

    public double ElapsedMilliseconds { get; init; }

    public double MessagesPerSecond { get; init; }
}
