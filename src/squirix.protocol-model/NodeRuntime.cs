using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct NodeRuntime(
    int CommitIndex,
    int AppliedIndex,
    int VotesGranted,
    int ReadIndex,
    int ReadAcks,
    bool ReadReady,
    bool Stopping,
    int KnownLeaderTerm = 0,
    int KnownLeaderId = -1,
    int? DurableCommitIndex = null)
{
    internal static readonly NodeRuntime Initial = Create(0, 0, 0, 0, 0, false);

    internal static NodeRuntime Create(int commitIndex, int appliedIndex, int votesGranted, int readIndex, int readAcks, bool readReady, bool stopping = false) => new(
        commitIndex,
        appliedIndex,
        votesGranted,
        readIndex,
        readAcks,
        readReady,
        stopping);
}
