namespace Squirix.Server.Node.Cluster.Membership;

internal sealed class Peer
{
    public required string NodeId { get; init; }

    public required string Url { get; init; }
}
