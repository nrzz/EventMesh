using BenchmarkDotNet.Attributes;
using EventMesh.Abstractions.Messaging;

namespace EventMesh.Benchmarks;

[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class LatencyBenchmark
{
    private IServiceProvider _provider = null!;
    private IMessageBus _messageBus = null!;
    private BenchmarkMessage _message = null!;
    private PublishOptions _publishOptions = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        (_provider, _messageBus) = await BenchmarkEnvironment.CreateAsync();
        _message = new BenchmarkMessage { Id = 1, Payload = "latency-benchmark" };
        _publishOptions = new PublishOptions
        {
            Topic = typeof(BenchmarkMessage).FullName,
            MessageType = typeof(BenchmarkMessage).FullName,
        };
    }

    [GlobalCleanup]
    public async Task GlobalCleanup() => await BenchmarkEnvironment.DisposeAsync(_provider);

    [Benchmark]
    public Task PublishLatency() => _messageBus.PublishAsync(_message, _publishOptions);
}
