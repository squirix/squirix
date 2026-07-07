using System;

namespace Squirix.Internal.Cluster.Transport;

internal sealed class Peer
{
    internal required Uri Uri { get; init; }

    internal required string NodeId { get; init; }
}
