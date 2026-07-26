using System;

namespace Squirix.Internal.Cluster.Transport;

internal sealed class Peer
{
    internal required string NodeId { get; init; }

    internal required Uri Uri { get; init; }
}
