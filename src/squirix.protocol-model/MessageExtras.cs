using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>The fields of a message beyond its route.</summary>
/// <param name="LastLogIndex">The index of the last entry of a vote request, or of the entry an append carries.</param>
/// <param name="LastLogTerm">The term of that entry.</param>
/// <param name="Success">Whether a reply grants or accepts.</param>
/// <param name="MatchIndex">The index an append reply confirms.</param>
/// <param name="ReadIndex">The read index of a read confirm.</param>
/// <param name="PrevLogTerm">The term of the entry before the one an append carries, or 0 for the first entry.</param>
[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct MessageExtras(int LastLogIndex, int LastLogTerm, bool Success, int MatchIndex, int ReadIndex, int PrevLogTerm = 0);
