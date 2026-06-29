using System;

namespace Squirix.Internal.Cluster.Transport;

internal sealed class Peer
{
    public required string NodeId { get; init; }

    public required Uri Uri { get; init; }
}
