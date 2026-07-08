using BenchmarkDotNet.Attributes;
using EventMesh.Abstractions.Messaging;

namespace EventMesh.Benchmarks;

[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class PublishBenchmark
{
    private IServiceProvider _provider = null!;
    private IMessageBus _messageBus = null!;
    private BenchmarkMessage _message = null!;
    private PublishOptions _publishOptions = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        (_provider, _messageBus) = await BenchmarkEnvironment.CreateAsync();
        _message = new BenchmarkMessage { Id = 1, Payload = "publish-throughput" };
        _publishOptions = new PublishOptions
        {
            Topic = typeof(BenchmarkMessage).FullName,
            MessageType = typeof(BenchmarkMessage).FullName,
        };
    }

    [GlobalCleanup]
    public async Task GlobalCleanup() => await BenchmarkEnvironment.DisposeAsync(_provider);

    [Benchmark]
    [Arguments(1)]
    [Arguments(10)]
    [Arguments(100)]
    public async Task PublishAsync(int messageCount)
    {
        for (var i = 0; i < messageCount; i++)
        {
            await _messageBus.PublishAsync(
                new BenchmarkMessage { Id = i, Payload = _message.Payload },
                _publishOptions);
        }
    }
}
