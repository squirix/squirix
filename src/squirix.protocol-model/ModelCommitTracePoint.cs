using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct ModelCommitTracePoint(int Term, int LogIndex, int CommitIndex, int AppliedIndex);
