using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.TestKit;

/// <summary>Retention failure metrics that record nothing, for tests that do not observe retention failures.</summary>
[Immutable]
internal sealed class NoOpRetentionFailureMetrics : IManifestRetentionFailureMetrics
{
    private NoOpRetentionFailureMetrics()
    {
    }

    /// <summary>Gets the shared instance.</summary>
    internal static NoOpRetentionFailureMetrics Instance { get; } = new();

    /// <inheritdoc />
    public void RecordDeleteFailure(string artifactKind, string outcome)
    {
    }
}
