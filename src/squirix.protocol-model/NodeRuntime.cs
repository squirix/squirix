using System.Runtime.InteropServices;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
internal readonly record struct NodeRuntime(int CommitIndex, int AppliedIndex, int VotesGranted, int ReadIndex, int ReadAcks, bool ReadReady, bool BadOldCommit)
{
    internal static readonly NodeRuntime Initial = Create(0, 0, 0, 0, 0, false, false);

    internal static NodeRuntime Create(int commitIndex, int appliedIndex, int votesGranted, int readIndex, int readAcks, bool readReady, bool badOldCommit) => new(
        commitIndex,
        appliedIndex,
        votesGranted,
        readIndex,
        readAcks,
        readReady,
        badOldCommit);
}
