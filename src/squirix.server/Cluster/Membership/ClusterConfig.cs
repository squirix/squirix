using System;
using System.Diagnostics.CodeAnalysis;

namespace Squirix.Server.Cluster.Membership;

internal sealed class ClusterConfig
{
    [SetsRequiredMembers]
    public ClusterConfig()
    {
        Peers = [];
    }

    [SetsRequiredMembers]
    public ClusterConfig(ServerPeer[] peers)
    {
        Peers = peers;
    }

    internal required string ClusterId { get; init; } = "cluster";

    internal required string NodeId { get; init; } = "node";

    internal ServerPeer[] Peers { get; }

    internal required Uri Uri { get; init; } = new("https://localhost:6001");

    internal int VirtualNodes { get; init; } = 128;
}
