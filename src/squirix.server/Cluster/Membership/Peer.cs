namespace Squirix.Server.Cluster.Membership;

internal sealed class Peer
{
    public string? InterNodeUrl { get; init; }

    public required string NodeId { get; init; }

    public required string Url { get; init; }
}
