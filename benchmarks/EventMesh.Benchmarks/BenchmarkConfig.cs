using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Horology;

namespace EventMesh.Benchmarks;

/// <summary>
/// Shared BenchmarkDotNet configuration for EventMesh benchmarks.
/// </summary>
public sealed class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        AddJob(Job.MediumRun
            .WithWarmupCount(3)
            .WithIterationCount(5)
            .WithLaunchCount(1));

        AddDiagnoser(MemoryDiagnoser.Default);
        AddColumn(StatisticColumn.Mean);
        AddColumn(StatisticColumn.StdDev);
        AddColumn(StatisticColumn.P50);
        AddColumn(StatisticColumn.P95);
        AddColumn(new P99Column());
        Orderer = new DefaultOrderer(SummaryOrderPolicy.FastestToSlowest);
    }

    private sealed class P99Column : IColumn
    {
        public string Id => "P99";

        public string ColumnName => "P99";

        public bool AlwaysShow => true;

        public ColumnCategory Category => ColumnCategory.Custom;

        public int PriorityInCategory => 0;

        public bool IsNumeric => true;

        public UnitType UnitType => UnitType.Time;

        public string Legend => "99th percentile";

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase) =>
            GetValue(summary, benchmarkCase, summary.Style);

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            var report = summary.Reports.FirstOrDefault(r => r.BenchmarkCase.Equals(benchmarkCase));
            var percentiles = report?.ResultStatistics?.Percentiles;
            if (percentiles is null)
            {
                return "N/A";
            }

            var nanoseconds = percentiles.Percentile(99);
            return TimeInterval.FromNanoseconds(nanoseconds).ToString();
        }

        public bool IsAvailable(Summary summary) => true;

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
    }
}
