using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

/// <summary>Metrics for best-effort manifest retention cleanup after durable manifest commits.</summary>
internal static class StorageRetentionMetrics
{
    internal static readonly Counter2Labels DeleteFailuresTotal = new(
        ServerMeterRegistry.Meter.CreateCounter<long>("squirix_storage_retention_delete_failures_total"),
        "artifact",
        "outcome");

    internal sealed record Counter2Labels(Counter<long> Counter, string Key1, string Key2)
    {
        internal ServerCounterLabelBinding WithLabels(string v1, string v2) => new(Counter, Key1, v1, Key2, v2);
    }
}
