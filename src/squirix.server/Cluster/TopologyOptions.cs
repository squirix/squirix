using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Cluster;

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

    internal required string NodeId { get; init; } = "node";

    internal ServerPeer[] Peers { get; }

    internal required Uri Uri { get; init; } = new("https://localhost:6001");

    internal int VirtualNodes { get; init; } = 128;
}
