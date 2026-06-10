using System.Diagnostics.CodeAnalysis;

namespace Squirix.Server.Cluster.Membership;

internal sealed class ClusterConfig
{
    [method: SetsRequiredMembers]
    public ClusterConfig()
    {
    }

    public required string ClusterId { get; init; } = "cluster";

    public required string NodeId { get; init; } = "node";

    public required Peer[] Peers { get; init; } = [];

    public required string Url { get; init; } = "https://localhost:6001";

    public int VirtualNodes { get; init; } = 128;
}
