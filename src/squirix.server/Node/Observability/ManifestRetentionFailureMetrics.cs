using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Node.Observability;

/// <summary>Emits manifest retention cleanup failure metrics for storage retention paths.</summary>
[Immutable]
internal sealed class ManifestRetentionFailureMetrics : IManifestRetentionFailureMetrics
{
    private readonly StorageRetentionMetrics _storageRetentionMetrics;

    internal ManifestRetentionFailureMetrics(Meter meter)
    {
        _storageRetentionMetrics = new StorageRetentionMetrics(meter);
    }

    /// <inheritdoc />
    public void RecordDeleteFailure(string artifactKind, string outcome) => _storageRetentionMetrics.IncrementDeleteFailuresTotal(artifactKind, outcome);

    /// <summary>Low-cardinality manifest retention cleanup metrics on the host-scoped <see cref="Meter" />.</summary>
    [Immutable]
    private sealed class StorageRetentionMetrics
    {
        private readonly Counter2Labels _deleteFailuresTotal;

        internal StorageRetentionMetrics(Meter meter)
        {
            _deleteFailuresTotal = new Counter2Labels(meter.CreateCounter<long>("squirix_storage_retention_delete_failures_total"), "artifact", "outcome");
        }

        internal void IncrementDeleteFailuresTotal(string artifactKind, string outcome, int increment = 1) => _deleteFailuresTotal.WithLabels(artifactKind, outcome).Inc(increment);

        [Immutable]
        private sealed record Counter2Labels(Counter<long> Counter, string Key1, string Key2)
        {
            internal ServerCounterLabelBinding WithLabels(string v1, string v2) => new(Counter, Key1, v1, Key2, v2);
        }
    }
}
