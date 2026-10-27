using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>The leader's view of one other copy of its replica group.</summary>
/// <param name="PeerId">The node identifier of the copy.</param>
/// <param name="State">The name of the participation state the leader holds for the copy.</param>
/// <param name="LagEntries">How many entries the copy is behind the last log index of the leader, by the match index the leader knows.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaPeerStatus(string PeerId, string State, ulong LagEntries);
