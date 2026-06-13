namespace Squirix.Server.Node.Observability;

internal static class CompactionMetrics
{
    // Labels: node, result (success|failure)
    public static readonly Histogram2Labels DurationSeconds = new(MeterRegistry.Meter.CreateHistogram<double>("squirix_compaction_duration_seconds"), "node", "result");
}
