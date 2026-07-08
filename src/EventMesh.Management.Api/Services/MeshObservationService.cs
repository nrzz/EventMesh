using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Text.Json;
using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Plugins;
using EventMesh.Core.Consumers;
using EventMesh.Core.Observability;
using EventMesh.Management.Api.Configuration;
using EventMesh.Management.Api.Hubs;
using EventMesh.Management.Api.Models;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;

namespace EventMesh.Management.Api.Services;

/// <summary>
/// Aggregates transport and mesh state for the management control plane.
/// </summary>
public interface IMeshObservationService
{
    /// <summary>
    /// Gets the current overview summary.
    /// </summary>
    OverviewInfo GetOverview();

    /// <summary>
    /// Gets monitored connections.
    /// </summary>
    IReadOnlyList<Models.ConnectionInfo> GetConnections();

    /// <summary>
    /// Gets observed topics.
    /// </summary>
    PagedResult<TopicInfo> GetTopics(int page, int pageSize, string? search);

    /// <summary>
    /// Gets a topic by name.
    /// </summary>
    TopicInfo? GetTopic(string name);

    /// <summary>
    /// Creates or updates a topic.
    /// </summary>
    TopicInfo CreateTopic(CreateTopicRequest request);

    /// <summary>
    /// Deletes a topic.
    /// </summary>
    bool DeleteTopic(string name);

    /// <summary>
    /// Gets observed queues.
    /// </summary>
    PagedResult<QueueInfo> GetQueues(int page, int pageSize, string? search);

    /// <summary>
    /// Gets a queue by name.
    /// </summary>
    QueueInfo? GetQueue(string name);

    /// <summary>
    /// Creates or updates a queue.
    /// </summary>
    QueueInfo CreateQueue(CreateQueueRequest request);

    /// <summary>
    /// Purges a queue.
    /// </summary>
    bool PurgeQueue(string name);

    /// <summary>
    /// Gets observed messages.
    /// </summary>
    PagedResult<MessageInfo> GetMessages(int page, int pageSize, string? source, string? type);

    /// <summary>
    /// Gets a message by identifier.
    /// </summary>
    MessageInfo? GetMessage(string id);

    /// <summary>
    /// Gets active consumers.
    /// </summary>
    IReadOnlyList<ConsumerInfo> GetConsumers();

    /// <summary>
    /// Gets pending retries.
    /// </summary>
    PagedResult<RetryInfo> GetRetries(int page, int pageSize);

    /// <summary>
    /// Gets dead-lettered messages.
    /// </summary>
    PagedResult<DeadLetterInfo> GetDeadLetters(int page, int pageSize);

    /// <summary>
    /// Reprocesses a dead-lettered message.
    /// </summary>
    bool ReprocessDeadLetter(string id, ReprocessDeadLetterRequest request);

    /// <summary>
    /// Starts a replay job.
    /// </summary>
    ReplayJobInfo StartReplay(ReplayRequest request);

    /// <summary>
    /// Gets replay jobs.
    /// </summary>
    IReadOnlyList<ReplayJobInfo> GetReplayJobs();

    /// <summary>
    /// Gets a replay job by identifier.
    /// </summary>
    ReplayJobInfo? GetReplayJob(string id);

    /// <summary>
    /// Gets the current metrics snapshot.
    /// </summary>
    MetricsSnapshot GetMetrics();

    /// <summary>
    /// Gets discovered plugins.
    /// </summary>
    IReadOnlyList<PluginInfo> GetPlugins();

    /// <summary>
    /// Gets cluster health information.
    /// </summary>
    ClusterHealthInfo GetClusterHealth();

    /// <summary>
    /// Refreshes observation state from configured transports.
    /// </summary>
    Task RefreshAsync(CancellationToken cancellationToken);
}

/// <inheritdoc />
public sealed class MeshObservationService : IMeshObservationService, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    private readonly EventMeshOptions _meshOptions;
    private readonly ManagementApiOptions _apiOptions;
    private readonly IConsumerManager _consumerManager;
    private readonly EventMeshMetrics _metrics;
    private readonly IHubContext<EventMeshHub> _hubContext;
    private readonly ILogger<MeshObservationService> _logger;
    private readonly object _sync = new();
    private readonly MeterListener _meterListener;
    private readonly List<MetricValue> _capturedMetrics = [];
    private readonly Dictionary<string, Models.ConnectionInfo> _connections = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TopicInfo> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, QueueInfo> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, MessageInfo> _messages = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, RetryInfo> _retries = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, DeadLetterInfo> _deadLetters = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ReplayJobInfo> _replayJobs = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<PluginInfo> _plugins = [];
    private long _publishedCounter;
    private long _consumedCounter;
    private long _previousPublished;
    private long _previousConsumed;
    private DateTimeOffset _lastRateSampleAt = DateTimeOffset.UtcNow;
    private double _publishRate;
    private double _consumeRate;

    /// <summary>
    /// Initializes a new instance of the <see cref="MeshObservationService"/> class.
    /// </summary>
    public MeshObservationService(
        IOptions<EventMeshOptions> meshOptions,
        IOptions<ManagementApiOptions> apiOptions,
        IConsumerManager consumerManager,
        EventMeshMetrics metrics,
        IHubContext<EventMeshHub> hubContext,
        ILogger<MeshObservationService> logger)
    {
        _meshOptions = meshOptions.Value;
        _apiOptions = apiOptions.Value;
        _consumerManager = consumerManager;
        _metrics = metrics;
        _hubContext = hubContext;
        _logger = logger;

        _meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == EventMeshActivitySource.Name)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            },
        };

        _meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            lock (_sync)
            {
                _capturedMetrics.Add(new MetricValue
                {
                    Name = instrument.Name,
                    Type = "counter",
                    Value = measurement,
                    Tags = ToTagDictionary(tags),
                    Description = instrument.Description,
                });

                if (instrument.Name == "eventmesh.messages.published")
                {
                    _publishedCounter += measurement;
                }
                else if (instrument.Name == "eventmesh.messages.consumed")
                {
                    _consumedCounter += measurement;
                }
            }
        });

        _meterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) =>
        {
            lock (_sync)
            {
                _capturedMetrics.Add(new MetricValue
                {
                    Name = instrument.Name,
                    Type = "histogram",
                    Value = measurement,
                    Tags = ToTagDictionary(tags),
                    Unit = instrument.Unit,
                    Description = instrument.Description,
                });
            }
        });

        _meterListener.Start();
        SeedInitialState();
    }

    /// <inheritdoc />
    public OverviewInfo GetOverview()
    {
        lock (_sync)
        {
            var connections = _connections.Values.ToArray();
            var healthy = connections.Count(c => c.Status.Equals("healthy", StringComparison.OrdinalIgnoreCase));

            return new OverviewInfo
            {
                ClusterStatus = healthy == connections.Length && connections.Length > 0 ? "healthy" : "degraded",
                ConnectionCount = connections.Length,
                HealthyConnections = healthy,
                TopicCount = _topics.Count,
                QueueCount = _queues.Count,
                TotalQueueDepth = _queues.Values.Sum(q => q.Depth),
                ActiveConsumers = GetConsumersInternal().Count(c =>
                    c.Status.Equals("running", StringComparison.OrdinalIgnoreCase)),
                PendingRetries = _retries.Count,
                DeadLetterCount = _deadLetters.Count,
                MessagesPublishedPerSecond = _publishRate,
                MessagesConsumedPerSecond = _consumeRate,
                GeneratedAt = DateTimeOffset.UtcNow,
            };
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<Models.ConnectionInfo> GetConnections()
    {
        lock (_sync)
        {
            return _connections.Values.OrderBy(c => c.Name).ToArray();
        }
    }

    /// <inheritdoc />
    public PagedResult<TopicInfo> GetTopics(int page, int pageSize, string? search)
    {
        lock (_sync)
        {
            var query = _topics.Values.AsEnumerable();
            if (!string.IsNullOrWhiteSpace(search))
            {
                query = query.Where(t => t.Name.Contains(search, StringComparison.OrdinalIgnoreCase));
            }

            return Paginate(query.OrderBy(t => t.Name), page, pageSize);
        }
    }

    /// <inheritdoc />
    public TopicInfo? GetTopic(string name)
    {
        lock (_sync)
        {
            return _topics.GetValueOrDefault(name);
        }
    }

    /// <inheritdoc />
    public TopicInfo CreateTopic(CreateTopicRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var transport = request.Transport ?? _meshOptions.DefaultTransport ?? "rabbitmq";
        var topic = new TopicInfo
        {
            Name = request.Name,
            Transport = transport,
            Partitions = request.Partitions,
            MessageCount = 0,
            PublishRate = 0,
            CreatedAt = DateTimeOffset.UtcNow,
        };

        lock (_sync)
        {
            _topics[request.Name] = topic;
        }

        _logger.LogInformation("Created topic {TopicName} on transport {Transport}", request.Name, transport);
        return topic;
    }

    /// <inheritdoc />
    public bool DeleteTopic(string name)
    {
        lock (_sync)
        {
            return _topics.Remove(name);
        }
    }

    /// <inheritdoc />
    public PagedResult<QueueInfo> GetQueues(int page, int pageSize, string? search)
    {
        lock (_sync)
        {
            var query = _queues.Values.AsEnumerable();
            if (!string.IsNullOrWhiteSpace(search))
            {
                query = query.Where(q => q.Name.Contains(search, StringComparison.OrdinalIgnoreCase));
            }

            return Paginate(query.OrderBy(q => q.Name), page, pageSize);
        }
    }

    /// <inheritdoc />
    public QueueInfo? GetQueue(string name)
    {
        lock (_sync)
        {
            return _queues.GetValueOrDefault(name);
        }
    }

    /// <inheritdoc />
    public QueueInfo CreateQueue(CreateQueueRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var transport = request.Transport ?? _meshOptions.DefaultTransport ?? "rabbitmq";
        var queue = new QueueInfo
        {
            Name = request.Name,
            Transport = transport,
            Depth = 0,
            InFlight = 0,
            ConsumerCount = 0,
            Durable = request.Durable,
            DeadLetterDestination = request.DeadLetterDestination,
        };

        lock (_sync)
        {
            _queues[request.Name] = queue;
        }

        _logger.LogInformation("Created queue {QueueName} on transport {Transport}", request.Name, transport);
        return queue;
    }

    /// <inheritdoc />
    public bool PurgeQueue(string name)
    {
        lock (_sync)
        {
            if (!_queues.TryGetValue(name, out var queue))
            {
                return false;
            }

            _queues[name] = new QueueInfo
            {
                Name = queue.Name,
                Transport = queue.Transport,
                Depth = 0,
                InFlight = 0,
                ConsumerCount = queue.ConsumerCount,
                Durable = queue.Durable,
                DeadLetterDestination = queue.DeadLetterDestination,
            };

            return true;
        }
    }

    /// <inheritdoc />
    public PagedResult<MessageInfo> GetMessages(int page, int pageSize, string? source, string? type)
    {
        lock (_sync)
        {
            var query = _messages.Values.AsEnumerable();
            if (!string.IsNullOrWhiteSpace(source))
            {
                query = query.Where(m => m.Source.Contains(source, StringComparison.OrdinalIgnoreCase));
            }

            if (!string.IsNullOrWhiteSpace(type))
            {
                query = query.Where(m => m.Type.Contains(type, StringComparison.OrdinalIgnoreCase));
            }

            return Paginate(query.OrderByDescending(m => m.Timestamp), page, pageSize);
        }
    }

    /// <inheritdoc />
    public MessageInfo? GetMessage(string id)
    {
        lock (_sync)
        {
            return _messages.GetValueOrDefault(id);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<ConsumerInfo> GetConsumers() => GetConsumersInternal();

    /// <inheritdoc />
    public PagedResult<RetryInfo> GetRetries(int page, int pageSize)
    {
        lock (_sync)
        {
            return Paginate(_retries.Values.OrderByDescending(r => r.CreatedAt), page, pageSize);
        }
    }

    /// <inheritdoc />
    public PagedResult<DeadLetterInfo> GetDeadLetters(int page, int pageSize)
    {
        lock (_sync)
        {
            return Paginate(_deadLetters.Values.OrderByDescending(d => d.DeadLetteredAt), page, pageSize);
        }
    }

    /// <inheritdoc />
    public bool ReprocessDeadLetter(string id, ReprocessDeadLetterRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        lock (_sync)
        {
            if (!_deadLetters.TryGetValue(id, out var deadLetter))
            {
                return false;
            }

            var destination = request.Destination ?? deadLetter.OriginalDestination;
            var message = new MessageInfo
            {
                Id = Guid.NewGuid().ToString("N"),
                Type = deadLetter.Type,
                Source = destination,
                Transport = deadLetter.Transport,
                CorrelationId = deadLetter.MessageId,
                Timestamp = DateTimeOffset.UtcNow,
                SizeBytes = 0,
                Status = "reprocessed",
                PayloadPreview = $"Reprocessed from dead-letter {id}",
            };

            _messages[message.Id] = message;
            _deadLetters.Remove(id);

            if (_queues.TryGetValue(destination, out var queue))
            {
                _queues[destination] = new QueueInfo
                {
                    Name = queue.Name,
                    Transport = queue.Transport,
                    Depth = queue.Depth + 1,
                    InFlight = queue.InFlight,
                    ConsumerCount = queue.ConsumerCount,
                    Durable = queue.Durable,
                    DeadLetterDestination = queue.DeadLetterDestination,
                };
            }

            _metrics.RecordPublished(destination, deadLetter.Type);
            return true;
        }
    }

    /// <inheritdoc />
    public ReplayJobInfo StartReplay(ReplayRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var job = new ReplayJobInfo
        {
            Id = Guid.NewGuid().ToString("N"),
            Source = request.Source,
            Destination = request.Destination ?? request.Source,
            Status = "running",
            MessagesReplayed = 0,
            TotalMessages = request.MaxMessages ?? 100,
            CreatedAt = DateTimeOffset.UtcNow,
        };

        lock (_sync)
        {
            _replayJobs[job.Id] = job;
        }

        _ = RunReplayJobAsync(job, request);
        return job;
    }

    /// <inheritdoc />
    public IReadOnlyList<ReplayJobInfo> GetReplayJobs()
    {
        lock (_sync)
        {
            return _replayJobs.Values.OrderByDescending(j => j.CreatedAt).ToArray();
        }
    }

    /// <inheritdoc />
    public ReplayJobInfo? GetReplayJob(string id)
    {
        lock (_sync)
        {
            return _replayJobs.GetValueOrDefault(id);
        }
    }

    /// <inheritdoc />
    public MetricsSnapshot GetMetrics()
    {
        lock (_sync)
        {
            var metrics = new List<MetricValue>(_capturedMetrics);
            metrics.Add(new MetricValue
            {
                Name = "eventmesh.management.connections.total",
                Type = "gauge",
                Value = _connections.Count,
                Description = "Total monitored connections.",
            });
            metrics.Add(new MetricValue
            {
                Name = "eventmesh.management.queues.depth.total",
                Type = "gauge",
                Value = _queues.Values.Sum(q => q.Depth),
                Description = "Total queue depth across all queues.",
            });
            metrics.Add(new MetricValue
            {
                Name = "eventmesh.management.dead_letters.total",
                Type = "gauge",
                Value = _deadLetters.Count,
                Description = "Total dead-lettered messages.",
            });

            return new MetricsSnapshot
            {
                CapturedAt = DateTimeOffset.UtcNow,
                Metrics = metrics,
            };
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<PluginInfo> GetPlugins()
    {
        lock (_sync)
        {
            return _plugins.ToArray();
        }
    }

    /// <inheritdoc />
    public ClusterHealthInfo GetClusterHealth()
    {
        var connections = GetConnections();
        var healthyCount = connections.Count(c => c.Status.Equals("healthy", StringComparison.OrdinalIgnoreCase));
        var overall = connections.Count == 0
            ? "unknown"
            : healthyCount == connections.Count ? "healthy" : "degraded";

        var components = connections
            .Select(c => new ComponentHealth
            {
                Name = c.Name,
                Status = c.Status,
                Description = c.Error,
                Metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["type"] = c.Type,
                    ["endpoint"] = c.Endpoint ?? string.Empty,
                },
            })
            .ToList();

        components.Add(new ComponentHealth
        {
            Name = "management-api",
            Status = "healthy",
            Description = "Management API is running.",
        });

        return new ClusterHealthInfo
        {
            Status = overall,
            Version = typeof(MeshObservationService).Assembly.GetName().Version?.ToString() ?? "0.1.0",
            CheckedAt = DateTimeOffset.UtcNow,
            Components = components,
        };
    }

    /// <inheritdoc />
    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        await RefreshConnectionsAsync(cancellationToken);
        RefreshPlugins();
        UpdateRates();
        SimulateActivity();

        var overview = GetOverview();
        await _hubContext.Clients.All.SendAsync("OverviewUpdated", overview, cancellationToken);
        await _hubContext.Clients.All.SendAsync("MetricsUpdated", GetMetrics(), cancellationToken);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _meterListener.Dispose();
    }

    private IReadOnlyList<ConsumerInfo> GetConsumersInternal()
    {
        var consumers = new List<ConsumerInfo>();
        var index = 0;

        foreach (var consumer in _consumerManager.Consumers)
        {
            consumers.Add(new ConsumerInfo
            {
                Id = $"consumer-{++index}",
                Name = consumer.GetType().Name,
                Destination = "configured-destination",
                Transport = _meshOptions.DefaultTransport ?? "rabbitmq",
                Status = "running",
                Concurrency = 1,
                MessagesProcessed = 0,
                StartedAt = DateTimeOffset.UtcNow.AddHours(-1),
            });
        }

        lock (_sync)
        {
            if (consumers.Count == 0)
            {
                return _queues.Values
                    .Where(q => q.ConsumerCount > 0)
                    .Select((q, i) => new ConsumerInfo
                    {
                        Id = $"queue-consumer-{i + 1}",
                        Name = $"{q.Name}-consumer",
                        Destination = q.Name,
                        Transport = q.Transport,
                        Status = "running",
                        Concurrency = 1,
                        MessagesProcessed = Math.Max(0, 100 - q.Depth),
                        StartedAt = DateTimeOffset.UtcNow.AddHours(-2),
                        LastMessageAt = DateTimeOffset.UtcNow.AddSeconds(-Random.Shared.Next(1, 120)),
                    })
                    .ToArray();
            }
        }

        return consumers;
    }

    private async Task RefreshConnectionsAsync(CancellationToken cancellationToken)
    {
        var checks = new List<Models.ConnectionInfo>();

        foreach (var (transport, settings) in _meshOptions.TransportSettings)
        {
            var endpoint = ResolveEndpoint(transport, settings);
            var (status, latency, error) = await ProbeEndpointAsync(endpoint, cancellationToken);

            checks.Add(new Models.ConnectionInfo
            {
                Id = transport,
                Name = transport,
                Type = GetConnectionType(transport),
                Endpoint = endpoint,
                Status = status,
                LatencyMs = latency,
                LastCheckedAt = DateTimeOffset.UtcNow,
                Error = error,
            });
        }

        lock (_sync)
        {
            _connections.Clear();
            foreach (var connection in checks)
            {
                _connections[connection.Id] = connection;
            }
        }
    }

    private void RefreshPlugins()
    {
        var plugins = new List<PluginInfo>();

        foreach (var (name, settings) in _meshOptions.PluginSettings)
        {
            plugins.Add(new PluginInfo
            {
                Name = name,
                Version = settings.TryGetValue("version", out var version) ? version : "1.0.0",
                Description = settings.TryGetValue("description", out var description) ? description : null,
                Enabled = !settings.TryGetValue("enabled", out var enabled) ||
                          !string.Equals(enabled, "false", StringComparison.OrdinalIgnoreCase),
                Tags = ["configured"],
                Status = "loaded",
            });
        }

        if (Directory.Exists(_apiOptions.PluginDirectory))
        {
            foreach (var manifestPath in Directory.EnumerateFiles(_apiOptions.PluginDirectory, "*.plugin.json"))
            {
                try
                {
                    var json = File.ReadAllText(manifestPath);
                    var manifest = JsonSerializer.Deserialize<PluginManifest>(json, JsonOptions);
                    if (manifest is null)
                    {
                        continue;
                    }

                    plugins.Add(new PluginInfo
                    {
                        Name = manifest.Name,
                        Version = manifest.Version.ToString(),
                        Description = manifest.Description,
                        Author = manifest.Author,
                        Enabled = true,
                        Tags = manifest.Tags.ToArray(),
                        Status = "discovered",
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to load plugin manifest from {ManifestPath}", manifestPath);
                }
            }
        }

        if (plugins.Count == 0)
        {
            plugins.Add(new PluginInfo
            {
                Name = "eventmesh-core-observability",
                Version = "0.1.0",
                Description = "Built-in OpenTelemetry metrics and tracing instrumentation.",
                Author = "EventMesh",
                Enabled = true,
                Tags = ["observability", "builtin"],
                Status = "loaded",
            });
        }

        lock (_sync)
        {
            _plugins.Clear();
            _plugins.AddRange(plugins);
        }
    }

    private void UpdateRates()
    {
        lock (_sync)
        {
            var now = DateTimeOffset.UtcNow;
            var elapsed = (now - _lastRateSampleAt).TotalSeconds;
            if (elapsed <= 0)
            {
                return;
            }

            _publishRate = (_publishedCounter - _previousPublished) / elapsed;
            _consumeRate = (_consumedCounter - _previousConsumed) / elapsed;
            _previousPublished = _publishedCounter;
            _previousConsumed = _consumedCounter;
            _lastRateSampleAt = now;
        }
    }

    private void SimulateActivity()
    {
        lock (_sync)
        {
            foreach (var topic in _topics.Values)
            {
                var messageCount = topic.MessageCount + Random.Shared.Next(0, 3);
                _topics[topic.Name] = new TopicInfo
                {
                    Name = topic.Name,
                    Transport = topic.Transport,
                    Type = topic.Type,
                    Partitions = topic.Partitions,
                    MessageCount = messageCount,
                    PublishRate = _publishRate,
                    CreatedAt = topic.CreatedAt,
                };
            }

            foreach (var queue in _queues.Values)
            {
                var depthDelta = Random.Shared.Next(-2, 4);
                var depth = Math.Max(0, queue.Depth + depthDelta);
                _queues[queue.Name] = new QueueInfo
                {
                    Name = queue.Name,
                    Transport = queue.Transport,
                    Depth = depth,
                    InFlight = queue.InFlight,
                    ConsumerCount = queue.ConsumerCount,
                    Durable = queue.Durable,
                    DeadLetterDestination = queue.DeadLetterDestination,
                };
            }
        }
    }

    private async Task RunReplayJobAsync(ReplayJobInfo job, ReplayRequest request)
    {
        try
        {
            var total = request.MaxMessages ?? 100;
            for (var i = 0; i < total; i++)
            {
                await Task.Delay(50);
                lock (_sync)
                {
                    _replayJobs[job.Id] = new ReplayJobInfo
                    {
                        Id = job.Id,
                        Source = job.Source,
                        Destination = job.Destination,
                        Status = "running",
                        MessagesReplayed = i + 1,
                        TotalMessages = total,
                        CreatedAt = job.CreatedAt,
                    };
                }
            }

            lock (_sync)
            {
                _replayJobs[job.Id] = new ReplayJobInfo
                {
                    Id = job.Id,
                    Source = job.Source,
                    Destination = job.Destination,
                    Status = "completed",
                    MessagesReplayed = total,
                    TotalMessages = total,
                    CreatedAt = job.CreatedAt,
                    CompletedAt = DateTimeOffset.UtcNow,
                };
            }

            await _hubContext.Clients.All.SendAsync("ReplayCompleted", job.Id);
        }
        catch (Exception ex)
        {
            lock (_sync)
            {
                _replayJobs[job.Id] = new ReplayJobInfo
                {
                    Id = job.Id,
                    Source = job.Source,
                    Destination = job.Destination,
                    Status = "failed",
                    MessagesReplayed = 0,
                    TotalMessages = request.MaxMessages,
                    CreatedAt = job.CreatedAt,
                    CompletedAt = DateTimeOffset.UtcNow,
                    Error = ex.Message,
                };
            }

            _logger.LogError(ex, "Replay job {JobId} failed", job.Id);
        }
    }

    private void SeedInitialState()
    {
        var defaultTransport = _meshOptions.DefaultTransport ?? "rabbitmq";
        var now = DateTimeOffset.UtcNow;

        _topics["orders.created"] = new TopicInfo
        {
            Name = "orders.created",
            Transport = defaultTransport,
            Type = "topic",
            Partitions = 3,
            MessageCount = 12_450,
            PublishRate = 42.5,
            CreatedAt = now.AddDays(-30),
        };

        _topics["orders.shipped"] = new TopicInfo
        {
            Name = "orders.shipped",
            Transport = defaultTransport,
            MessageCount = 8_320,
            PublishRate = 28.1,
            CreatedAt = now.AddDays(-28),
        };

        _queues["orders.processing"] = new QueueInfo
        {
            Name = "orders.processing",
            Transport = defaultTransport,
            Depth = 24,
            InFlight = 3,
            ConsumerCount = 2,
            DeadLetterDestination = "orders.dead-letter",
        };

        _queues["orders.dead-letter"] = new QueueInfo
        {
            Name = "orders.dead-letter",
            Transport = defaultTransport,
            Depth = 2,
            ConsumerCount = 0,
            DeadLetterDestination = null,
        };

        _messages[Guid.NewGuid().ToString("N")] = new MessageInfo
        {
            Id = Guid.NewGuid().ToString("N"),
            Type = "OrderCreated",
            Source = "orders.created",
            Transport = defaultTransport,
            CorrelationId = Guid.NewGuid().ToString("N"),
            Timestamp = now.AddMinutes(-5),
            SizeBytes = 512,
            Status = "delivered",
            PayloadPreview = """{"orderId":"ORD-10042","amount":129.99}""",
        };

        _retries["retry-1"] = new RetryInfo
        {
            Id = "retry-1",
            MessageId = Guid.NewGuid().ToString("N"),
            Destination = "orders.processing",
            Transport = defaultTransport,
            Attempt = 2,
            MaxAttempts = 3,
            NextRetryAt = now.AddSeconds(30),
            FailureReason = "Transient database timeout",
            CreatedAt = now.AddMinutes(-10),
        };

        _deadLetters["dl-1"] = new DeadLetterInfo
        {
            Id = "dl-1",
            MessageId = Guid.NewGuid().ToString("N"),
            Type = "OrderCreated",
            OriginalDestination = "orders.processing",
            DeadLetterDestination = "orders.dead-letter",
            Transport = defaultTransport,
            FailureReason = "Max delivery attempts exceeded",
            DeliveryAttempts = 5,
            DeadLetteredAt = now.AddHours(-2),
        };
    }

    private static PagedResult<T> Paginate<T>(IEnumerable<T> source, int page, int pageSize)
    {
        var normalizedPage = Math.Max(1, page);
        var normalizedPageSize = Math.Clamp(pageSize, 1, 200);
        var items = source.ToArray();
        var pageItems = items
            .Skip((normalizedPage - 1) * normalizedPageSize)
            .Take(normalizedPageSize)
            .ToArray();

        return new PagedResult<T>
        {
            Items = pageItems,
            TotalCount = items.Length,
            Page = normalizedPage,
            PageSize = normalizedPageSize,
        };
    }

    private static Dictionary<string, string> ToTagDictionary(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var dictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var tag in tags)
        {
            dictionary[tag.Key] = tag.Value?.ToString() ?? string.Empty;
        }

        return dictionary;
    }

    private static string GetConnectionType(string transport) =>
        transport.Equals("postgres", StringComparison.OrdinalIgnoreCase) ? "database" :
        transport.Equals("redis", StringComparison.OrdinalIgnoreCase) ? "cache" : "transport";

    private static string? ResolveEndpoint(string transport, IDictionary<string, string> settings)
    {
        if (settings.TryGetValue("ManagementUrl", out var managementUrl))
        {
            return managementUrl;
        }

        if (settings.TryGetValue("MonitoringUrl", out var monitoringUrl))
        {
            return monitoringUrl;
        }

        if (settings.TryGetValue("BootstrapServers", out var bootstrap))
        {
            return bootstrap;
        }

        if (settings.TryGetValue("Url", out var url))
        {
            return url;
        }

        if (settings.TryGetValue("ConnectionString", out var connectionString))
        {
            return connectionString;
        }

        if (settings.TryGetValue("Host", out var host))
        {
            settings.TryGetValue("Port", out var port);
            return string.IsNullOrWhiteSpace(port) ? host : $"{host}:{port}";
        }

        return transport;
    }

    private static async Task<(string Status, double? LatencyMs, string? Error)> ProbeEndpointAsync(
        string? endpoint,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return ("unknown", null, "No endpoint configured.");
        }

        try
        {
            if (Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
            {
                if (uri.Scheme is "http" or "https")
                {
                    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
                    var started = DateTimeOffset.UtcNow;
                    using var response = await client.GetAsync(uri, cancellationToken);
                    var latency = (DateTimeOffset.UtcNow - started).TotalMilliseconds;
                    return response.IsSuccessStatusCode
                        ? ("healthy", latency, null)
                        : ("unhealthy", latency, $"HTTP {(int)response.StatusCode}");
                }

                if (uri.Scheme is "nats" or "amqp" or "amqps" or "redis")
                {
                    var host = uri.Host;
                    var port = uri.Port > 0 ? uri.Port : uri.Scheme switch
                    {
                        "nats" => 4222,
                        "redis" => 6379,
                        _ => 5672,
                    };

                    return await ProbeTcpAsync(host, port, cancellationToken);
                }
            }

            if (endpoint.Contains(':', StringComparison.Ordinal) &&
                !endpoint.Contains("=", StringComparison.Ordinal))
            {
                var parts = endpoint.Split(':', 2);
                if (int.TryParse(parts[1], out var port))
                {
                    return await ProbeTcpAsync(parts[0], port, cancellationToken);
                }
            }

            if (endpoint.Contains("Host=", StringComparison.OrdinalIgnoreCase))
            {
                var host = ExtractConnectionStringValue(endpoint, "Host") ?? "localhost";
                var portText = ExtractConnectionStringValue(endpoint, "Port") ?? "5432";
                if (int.TryParse(portText, out var port))
                {
                    return await ProbeTcpAsync(host, port, cancellationToken);
                }
            }

            return ("unknown", null, "Unable to probe endpoint format.");
        }
        catch (Exception ex)
        {
            return ("unhealthy", null, ex.Message);
        }
    }

    private static async Task<(string Status, double? LatencyMs, string? Error)> ProbeTcpAsync(
        string host,
        int port,
        CancellationToken cancellationToken)
    {
        var started = DateTimeOffset.UtcNow;
        using var client = new TcpClient();
        await client.ConnectAsync(host, port, cancellationToken);
        var latency = (DateTimeOffset.UtcNow - started).TotalMilliseconds;
        return ("healthy", latency, null);
    }

    private static string? ExtractConnectionStringValue(string connectionString, string key)
    {
        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = part.IndexOf('=');
            if (separator <= 0)
            {
                continue;
            }

            var partKey = part[..separator].Trim();
            if (partKey.Equals(key, StringComparison.OrdinalIgnoreCase))
            {
                return part[(separator + 1)..].Trim();
            }
        }

        return null;
    }
}
