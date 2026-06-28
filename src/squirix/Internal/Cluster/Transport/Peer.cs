using System;

namespace Squirix.Internal.Cluster.Transport;

internal sealed class Peer
{
    internal required string NodeId { get; init; }

    public required Uri Url { get; init; }
}
