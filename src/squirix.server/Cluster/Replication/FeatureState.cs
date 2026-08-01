namespace Squirix.Server.Cluster.Replication;

/// <summary>Internal activation gate for the replication network path.</summary>
internal sealed class FeatureState
{
    private FeatureState(bool networkReplicationEnabled)
    {
        NetworkReplicationEnabled = networkReplicationEnabled;
    }

    /// <summary>Gets the shared disabled state until M8-09 activates RF&gt;1 networking.</summary>
    internal static FeatureState Disabled { get; } = new(false);

    /// <summary>Gets a value indicating whether inter-node replication RPCs may run.</summary>
    internal bool NetworkReplicationEnabled { get; }
}
