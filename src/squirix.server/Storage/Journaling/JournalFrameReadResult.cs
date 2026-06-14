using System.Runtime.InteropServices;

namespace Squirix.Server.Storage.Journaling;

[StructLayout(LayoutKind.Auto)]
internal readonly record struct JournalFrameReadResult(JournalFrameReadStatus Status, long FrameOffset, long NextFrameOffset);
