namespace Squirix.Server.Cluster.Membership;

internal sealed class Peer
{
    /// <summary>
    /// Gets the dedicated inter-node mTLS gRPC URL. When unset, the local internal listen port is applied to the peer host.
    /// </summary>
    public string? InterNodeUrl { get; init; }

    public required string NodeId { get; init; }

    public required string Url { get; init; }
}
