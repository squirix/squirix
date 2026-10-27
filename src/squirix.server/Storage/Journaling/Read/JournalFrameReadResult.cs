using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling.Read;

[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct JournalFrameReadResult(JournalFrameReadStatus Status, long NextFrameOffset);
