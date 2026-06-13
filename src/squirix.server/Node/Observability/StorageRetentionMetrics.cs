using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Metrics for best-effort manifest retention cleanup after durable manifest commits.
/// </summary>
internal static class StorageRetentionMetrics
{
    public static readonly Counter2Labels DeleteFailuresTotal = new(
        MeterRegistry.Meter.CreateCounter<long>("squirix_storage_retention_delete_failures_total"),
        "artifact",
        "outcome");

    internal readonly struct Counter2Labels
    {
        private readonly Counter<long> _ctr;
        private readonly string _k1;
        private readonly string _k2;

        public Counter2Labels(Counter<long> ctr, string k1, string k2)
        {
            _ctr = ctr;
            _k1 = k1;
            _k2 = k2;
        }

        public CounterLabelBinding WithLabels(string v1, string v2) => new(_ctr, _k1, v1, _k2, v2);
    }
}
