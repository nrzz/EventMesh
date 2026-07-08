namespace EventMesh.Benchmarks;

internal sealed class BenchmarkMessage
{
    public int Id { get; init; }

    public string Payload { get; init; } = "benchmark-payload";
}
