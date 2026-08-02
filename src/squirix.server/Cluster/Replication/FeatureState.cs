namespace Squirix.Server.Cluster.Replication;

/// <summary>Internal activation gate for the replication network path.</summary>
/// <param name="NetworkReplicationEnabled">Whether inter-node replication RPCs may run.</param>
internal readonly record struct FeatureState(bool NetworkReplicationEnabled)
{
    /// <summary>Gets the shared disabled state until M8-09 activates RF&gt;1 networking.</summary>
    internal static FeatureState Disabled { get; } = new(false);
}
