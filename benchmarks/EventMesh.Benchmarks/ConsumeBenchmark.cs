using BenchmarkDotNet.Attributes;
using EventMesh.Abstractions.Messaging;

namespace EventMesh.Benchmarks;

[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class ConsumeBenchmark
{
    private const int BatchSize = 100;

    private IServiceProvider _provider = null!;
    private IMessageBus _messageBus = null!;
    private IMessageConsumer _consumer = null!;
    private PublishOptions _publishOptions = null!;
    private int _consumedCount;
    private TaskCompletionSource _batchComplete = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        (_provider, _messageBus) = await BenchmarkEnvironment.CreateAsync();
        _publishOptions = new PublishOptions
        {
            Topic = typeof(BenchmarkMessage).FullName,
            MessageType = typeof(BenchmarkMessage).FullName,
        };

        _consumer = await _messageBus.SubscribeAsync<BenchmarkMessage>(
            (_, _) =>
            {
                var count = Interlocked.Increment(ref _consumedCount);
                if (count == BatchSize)
                {
                    _batchComplete.TrySetResult();
                }

                return Task.CompletedTask;
            },
            new SubscribeOptions
            {
                Topic = typeof(BenchmarkMessage).FullName,
                AutoAcknowledge = true,
                MaxConcurrency = 4,
            });

        await _consumer.StartAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _consumer.DisposeAsync();
        await BenchmarkEnvironment.DisposeAsync(_provider);
    }

    [IterationSetup]
    public async Task IterationSetup()
    {
        _consumedCount = 0;
        _batchComplete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await Task.Delay(5);
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public async Task ConsumeThroughput()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            await _messageBus.PublishAsync(
                new BenchmarkMessage { Id = i, Payload = "consume-throughput" },
                _publishOptions);
        }

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await _batchComplete.Task.WaitAsync(timeout.Token);
    }
}
