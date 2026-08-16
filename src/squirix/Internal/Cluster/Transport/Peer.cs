using System;
using Squirix.Attributes;

namespace Squirix.Internal.Cluster.Transport;

[Immutable]
internal sealed class Peer
{
    internal required string NodeId { get; init; }

    internal required Uri Uri { get; init; }
}
