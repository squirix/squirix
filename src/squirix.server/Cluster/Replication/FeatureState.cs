using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Internal activation gate for the replication network path.</summary>
/// <param name="NetworkReplicationEnabled">Whether internode replication RPCs may mutate state.</param>
/// <param name="FoundationOnly">
/// Whether the closed replication service is mapped for transport/identity tests without enabling RF&gt;1 mutations.
/// </param>
[Immutable]
internal readonly record struct FeatureState(bool NetworkReplicationEnabled, bool FoundationOnly)
{
    /// <summary>Gets the shared disabled state for RF=1 hosts.</summary>
    internal static FeatureState Disabled { get; } = new(false, false);

    /// <summary>Gets the activated state for RF&gt;1 hosts whose persistence and mTLS prerequisites passed.</summary>
    internal static FeatureState Activated { get; } = new(true, false);

    /// <summary>Gets foundation-only state used by testkit for transport tests without RF&gt;1 mutations.</summary>
    internal static FeatureState Foundation { get; } = new(false, true);
}
