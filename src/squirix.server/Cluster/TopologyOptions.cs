using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.Attributes;

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

    /// <summary>Gets the stopped-topology configuration generation (must be greater than zero).</summary>
    internal ulong ConfigurationGeneration { get; init; } = 1;

    internal required string NodeId { get; init; } = "node";

    internal ServerPeer[] Peers { get; }

    /// <summary>Gets the configured replica factor including the original owner (default 1).</summary>
    internal int ReplicaCount { get; init; } = 1;

    internal required Uri Uri { get; init; } = new("https://localhost:6001");

    internal int VirtualNodes { get; init; } = 128;
}
