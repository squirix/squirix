namespace Squirix.Server.Storage.Manifest;

/// <summary>No-op retention failure metrics used when observability wiring is absent.</summary>
internal sealed class NoOpManifestRetentionFailureMetrics : IManifestRetentionFailureMetrics
{
    internal static NoOpManifestRetentionFailureMetrics Instance { get; } = new();

    public void RecordDeleteFailure(string artifactKind, string outcome)
    {
        _ = artifactKind;
        _ = outcome;
    }
}
