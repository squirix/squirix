using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>One observation of a leader in a production trace; a field set to <see cref="NotObserved" /> is not checked.</summary>
/// <param name="Term">The leader's term.</param>
/// <param name="LogIndex">The last log index.</param>
/// <param name="CommitIndex">The commit index.</param>
/// <param name="AppliedIndex">The applied index, or <see cref="NotObserved" /> to skip the check.</param>
/// <param name="LeaderId">The leader's slot; slot k is model node k.</param>
/// <param name="LastLogTerm">The term of the last log entry.</param>
/// <param name="Votes">The votes the election counted; the model needs at most this many.</param>
/// <param name="Acks">
/// The copies stored when the commit index rose; the model needs at most this many. Set it only on the point recorded when the commit index
/// rose, with <paramref name="AppliedIndex" /> as it was before the apply or <see cref="NotObserved" />; a later point with Acks never matches.
/// </param>
/// <param name="ReadIndex">The read index of a served read.</param>
/// <param name="ReadAcks">The read confirmations counted when the read was served; the model needs at most this many.</param>
[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct ModelCommitTracePoint(
    int Term,
    int LogIndex,
    int CommitIndex,
    int AppliedIndex,
    int LeaderId = ModelCommitTracePoint.NotObserved,
    int LastLogTerm = ModelCommitTracePoint.NotObserved,
    int Votes = ModelCommitTracePoint.NotObserved,
    int Acks = ModelCommitTracePoint.NotObserved,
    int ReadIndex = ModelCommitTracePoint.NotObserved,
    int ReadAcks = ModelCommitTracePoint.NotObserved)
{
    /// <summary>The value of a field the trace does not observe.</summary>
    internal const int NotObserved = -1;
}
