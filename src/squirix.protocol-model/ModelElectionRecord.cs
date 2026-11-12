using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>Checker evidence that a node held leadership in a term, kept after that node leaves the role.</summary>
/// <param name="Term">The term the node led.</param>
/// <param name="NodeId">The elected node.</param>
[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct ModelElectionRecord(int Term, int NodeId);
