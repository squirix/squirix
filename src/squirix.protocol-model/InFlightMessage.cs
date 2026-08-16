using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct InFlightMessage(int Id, MsgKind Kind, int From, int To, int Term, int LastLogIndex, int LastLogTerm, bool Success, int MatchIndex, int ReadIndex)
{
    internal InFlightMessage(int id, MessagePayload payload)
        : this(id, payload.Kind, payload.From, payload.To, payload.Term, payload.LastLogIndex, payload.LastLogTerm, payload.Success, payload.MatchIndex, payload.ReadIndex)
    {
    }
}
