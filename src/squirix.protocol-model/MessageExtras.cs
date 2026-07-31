using System.Runtime.InteropServices;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
internal readonly record struct MessageExtras(int LastLogIndex, int LastLogTerm, bool Success, int MatchIndex, int ReadIndex);
