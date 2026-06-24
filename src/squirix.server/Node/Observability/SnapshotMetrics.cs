namespace Squirix.Server.Node.Observability;

internal static class SnapshotMetrics
{
    /// <summary>
    /// Labels: node, result (success|failure).
    /// </summary>
    public static readonly Histogram2Labels DurationSeconds = new(MeterRegistry.Meter.CreateHistogram<double>("squirix_snapshot_duration_seconds"), "node", "result");
}
