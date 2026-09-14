using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Side-table record for a tracked append. The journal work item itself stays immutable.</summary>
[Immutable]
internal sealed class PendingAppendEntry
{
    internal PendingAppendEntry(JournalWorkItem item, byte[] frameBytes, int frameLength, TaskCompletionSource? ack)
    {
        Item = item;
        FrameBytes = frameBytes;
        FrameLength = frameLength;
        Ack = ack;
    }

    /// <summary>Gets the ack resolved on completion or failure. May be <see langword="null" />.</summary>
    internal TaskCompletionSource? Ack { get; }

    /// <summary>Gets the encoded journal frame bytes owned until completion or drain.</summary>
    internal byte[] FrameBytes { get; }

    /// <summary>Gets the exact length of the framed payload inside <see cref="FrameBytes" />.</summary>
    internal int FrameLength { get; }

    /// <summary>Gets the tracked append work item.</summary>
    internal JournalWorkItem Item { get; }
}
