using System.Runtime.InteropServices;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
internal readonly record struct AppendOutcome(bool Success, int MatchIndex);
