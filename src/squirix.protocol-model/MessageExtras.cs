using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct MessageExtras(int LastLogIndex, int LastLogTerm, bool Success, int MatchIndex, int ReadIndex);
