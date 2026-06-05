using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal static class CompactionMetrics
{
    // Labels: node, result (success|failure)
    public static readonly Histogram2Labels DurationSeconds;

    private static readonly Histogram<double> DurationSecondsHist = MeterRegistry.Meter.CreateHistogram<double>("squirix_compaction_duration_seconds");

    static CompactionMetrics()
    {
        DurationSeconds = new Histogram2Labels(DurationSecondsHist, "node", "result");
    }
}
