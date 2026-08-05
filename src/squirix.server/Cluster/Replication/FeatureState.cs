namespace Squirix.Server.Cluster.Replication;

/// <summary>Internal activation gate for the replication network path.</summary>
/// <param name="NetworkReplicationEnabled">Whether internode replication RPCs may mutate state.</param>
/// <param name="FoundationOnly">
/// Whether the closed replication service is mapped for transport/identity tests without enabling RF&gt;1 mutations.
/// </param>
internal readonly record struct FeatureState(bool NetworkReplicationEnabled, bool FoundationOnly)
{
    /// <summary>Gets the shared disabled state until M8-09 activates RF&gt;1 networking.</summary>
    internal static FeatureState Disabled { get; } = new(false, false);

    /// <summary>Gets foundation-only state used by testkit before M8-09.</summary>
    internal static FeatureState Foundation { get; } = new(false, true);
}
