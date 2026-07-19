using System;

namespace Squirix.Server.Cluster.Transport;

internal sealed class ServerPeer
{
    internal required string NodeId { get; init; }

    internal required Uri Uri { get; init; }

    /// <summary>Gets the dedicated inter-node mTLS gRPC URL. When unset, the local internal listen port is applied to the peer host.</summary>
    internal Uri? InterNodeUri { get; init; }
}
