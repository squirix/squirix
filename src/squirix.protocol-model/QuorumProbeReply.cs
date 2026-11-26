using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct QuorumProbeReply(int Slot, int RequestTerm, int ReplyTerm, QuorumProbeVerdict Verdict);
