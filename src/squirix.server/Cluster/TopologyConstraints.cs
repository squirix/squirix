namespace Squirix.Server.Cluster;

/// <summary>Cluster-wide topology limits shared by validation and replication policy.</summary>
internal static class TopologyConstraints
{
    /// <summary>Maximum supported replica factor for preview.8.</summary>
    internal const int MaxReplicaCount = 5;
}
