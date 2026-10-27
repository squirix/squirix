using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct NodeRuntime(int CommitIndex, int AppliedIndex, int VotesGranted, int ReadIndex, int ReadAcks, bool ReadReady)
{
    internal static readonly NodeRuntime Initial = Create(0, 0, 0, 0, 0, false);

    internal static NodeRuntime Create(int commitIndex, int appliedIndex, int votesGranted, int readIndex, int readAcks, bool readReady) => new(
        commitIndex,
        appliedIndex,
        votesGranted,
        readIndex,
        readAcks,
        readReady);
}
