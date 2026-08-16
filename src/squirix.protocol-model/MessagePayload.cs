using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct MessagePayload(MsgKind Kind, int From, int To, int Term, int LastLogIndex, int LastLogTerm, bool Success, int MatchIndex, int ReadIndex)
{
    internal MessagePayload(MessageRoute route, MessageExtras extras)
        : this(route.Kind, route.From, route.To, route.Term, extras.LastLogIndex, extras.LastLogTerm, extras.Success, extras.MatchIndex, extras.ReadIndex)
    {
    }

    internal static MessagePayload Append(int from, int to, int term, int lastLogIndex, int lastLogTerm, int matchIndex) => new(
        new MessageRoute(MsgKind.AppendEntries, from, to, term),
        new MessageExtras(lastLogIndex, lastLogTerm, false, matchIndex, 0));

    internal static MessagePayload AppendResponse(int from, int to, int term, int lastLogIndex, int lastLogTerm, bool success, int matchIndex) => new(
        new MessageRoute(MsgKind.AppendResponse, from, to, term),
        new MessageExtras(lastLogIndex, lastLogTerm, success, matchIndex, 0));

    internal static MessagePayload ReadRequest(int from, int to, int term, int readIndex) => new(
        new MessageRoute(MsgKind.ReadIndexRequest, from, to, term),
        new MessageExtras(0, 0, false, 0, readIndex));

    internal static MessagePayload ReadResponse(int from, int to, int term, bool ok, int readIndex) => new(
        new MessageRoute(MsgKind.ReadIndexResponse, from, to, term),
        new MessageExtras(0, 0, ok, 0, readIndex));

    internal static MessagePayload VoteRequest(int from, int to, int term, int lastLogIndex, int lastLogTerm) => new(
        new MessageRoute(MsgKind.RequestVote, from, to, term),
        new MessageExtras(lastLogIndex, lastLogTerm, false, 0, 0));

    internal static MessagePayload VoteResponse(int from, int to, int term, bool grant) => new(
        new MessageRoute(MsgKind.VoteResponse, from, to, term),
        new MessageExtras(0, 0, grant, 0, 0));
}
