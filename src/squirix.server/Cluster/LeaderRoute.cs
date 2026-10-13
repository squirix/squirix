using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

/// <summary>The leader of a replica group as this node knows it.</summary>
/// <param name="NodeId">The identifier of the leader node.</param>
/// <param name="Term">The term it leads.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct LeaderRoute(string NodeId, ulong Term)
{
    /// <summary>Tells whether this route names another node as the leader of a term; a term has one leader.</summary>
    /// <param name="nodeId">The node that claims the term.</param>
    /// <param name="term">The claimed term.</param>
    /// <returns><see langword="true" /> when a leader is known for <paramref name="term" /> and it is not <paramref name="nodeId" />.</returns>
    internal bool LeadsTermInstead(string nodeId, ulong term) =>
        !string.IsNullOrEmpty(NodeId) && Term == term && !string.Equals(NodeId, nodeId, StringComparison.Ordinal);
}
