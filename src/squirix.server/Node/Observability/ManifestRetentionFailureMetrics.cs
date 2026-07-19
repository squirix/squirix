using System.Diagnostics.Metrics;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Node.Observability;

/// <summary>Emits manifest retention cleanup failure metrics for storage retention paths.</summary>
internal sealed class ManifestRetentionFailureMetrics : IManifestRetentionFailureMetrics
{
    internal static ManifestRetentionFailureMetrics Instance { get; } = new();

    /// <inheritdoc />
    public void RecordDeleteFailure(string artifactKind, string outcome) =>
        StorageRetentionMetrics.IncrementDeleteFailuresTotal(artifactKind, outcome);

    /// <summary>Low-cardinality manifest retention cleanup metrics on the shared <see cref="ServerMeterRegistry.Meter" />.</summary>
    private static class StorageRetentionMetrics
    {
        private static readonly Counter2Labels DeleteFailuresTotal = new(
            ServerMeterRegistry.Meter.CreateCounter<long>("squirix_storage_retention_delete_failures_total"),
            "artifact",
            "outcome");

        internal static void IncrementDeleteFailuresTotal(string artifactKind, string outcome, int increment = 1) =>
            DeleteFailuresTotal.WithLabels(artifactKind, outcome).Inc(increment);

        private sealed record Counter2Labels(Counter<long> Counter, string Key1, string Key2)
        {
            internal ServerCounterLabelBinding WithLabels(string v1, string v2) => new(Counter, Key1, v1, Key2, v2);
        }
    }
}
