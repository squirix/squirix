using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Node.Observability;

/// <summary>Emits manifest retention cleanup failure metrics for storage retention paths.</summary>
internal sealed class OtelManifestRetentionMetrics : IManifestRetentionFailureMetrics
{
    public void RecordDeleteFailure(string artifactKind, string outcome) => StorageRetentionMetrics.DeleteFailuresTotal.WithLabels(artifactKind, outcome).Inc(1);
}
