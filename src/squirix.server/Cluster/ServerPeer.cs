using System;
using Squirix.Attributes;

namespace Squirix.Server.Cluster;

[Immutable]
internal sealed class ServerPeer
{
    /// <summary>Gets the dedicated inter-node mTLS gRPC URI. When unset, the local internal listen port is applied to the peer host.</summary>
    internal Uri? InterNodeUri { get; init; }

    internal required string NodeId { get; init; }

    internal required Uri Uri { get; init; }
}
