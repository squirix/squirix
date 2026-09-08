using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

[Immutable]
internal sealed class TopologyOptions
{
    [SetsRequiredMembers]
    internal TopologyOptions(ServerPeer[] peers)
    {
        Peers = peers;
    }

    [SetsRequiredMembers]
    internal TopologyOptions(ServerPeer peer)
    {
        Peers = [peer];
    }

    internal required string ClusterId { get; init; } = "cluster";

    /// <summary>Gets a value indicating whether automatic failover may trigger leader election. Always disabled until failover activation.</summary>
    internal bool AutomaticFailoverEnabled { get; init; }

    /// <summary>Gets a value indicating whether quorum reads require majority confirmation. Always disabled until quorum-read activation.</summary>
    internal bool QuorumReadsEnabled { get; init; }

    /// <summary>Gets the stopped-topology configuration generation (must be greater than zero).</summary>
    internal ulong ConfigurationGeneration { get; init; } = 1;

    internal required string NodeId { get; init; } = "node";

    internal ServerPeer[] Peers { get; }

    /// <summary>Gets the configured replica factor including the original owner (default 1).</summary>
    internal int ReplicaCount { get; init; } = 1;

    /// <summary>Gets a value indicating whether the operator explicitly opted into RF&gt;1 replication (default false).</summary>
    internal bool ReplicationEnabled { get; init; }

    internal required Uri Uri { get; init; } = new("https://localhost:6001");

    internal int VirtualNodes { get; init; } = 128;
}
