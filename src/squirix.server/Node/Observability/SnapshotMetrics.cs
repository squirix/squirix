namespace Squirix.Server.Node.Observability;

internal static class SnapshotMetrics
{
    /// <summary>
    /// Labels: node, result (success|failure).
    /// </summary>
    internal static readonly ServerHistogram2Labels DurationSeconds = new(ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_snapshot_duration_seconds"), "node", "result");
}
