using System.Runtime.InteropServices;

namespace Squirix.Server.Storage.Journaling.JsonFramed;

[StructLayout(LayoutKind.Auto)]
internal readonly struct JournalFrameReadResult
{
    internal JournalFrameReadResult(JournalFrameReadStatus status, long nextFrameOffset)
    {
        Status = status;
        NextFrameOffset = nextFrameOffset;
    }

    internal JournalFrameReadStatus Status { get; }

    internal long NextFrameOffset { get; }
}
