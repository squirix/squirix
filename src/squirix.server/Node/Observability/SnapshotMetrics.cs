using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal static class SnapshotMetrics
{
    // Labels: node, result (success|failure)
    public static readonly Histogram2Labels DurationSeconds;

    private static readonly Histogram<double> DurationSecondsHist = MeterRegistry.Meter.CreateHistogram<double>("squirix_snapshot_duration_seconds");

    static SnapshotMetrics()
    {
        DurationSeconds = new Histogram2Labels(DurationSecondsHist, "node", "result");
    }
}
