using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

/// <summary>The leader of a replica group as this node knows it.</summary>
/// <param name="NodeId">The identifier of the leader node.</param>
/// <param name="Term">The term it leads.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct LeaderRoute(string NodeId, ulong Term);
