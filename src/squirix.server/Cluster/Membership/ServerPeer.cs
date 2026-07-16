using System;

namespace Squirix.Server.Cluster.Membership;

internal sealed class ServerPeer
{
    public required string NodeId { get; init; }

    public required Uri Uri { get; init; }

    /// <summary>Gets the dedicated inter-node mTLS gRPC URL. When unset, the local internal listen port is applied to the peer host.</summary>
    public Uri? InterNodeUri { get; init; }
}
