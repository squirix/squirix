namespace Squirix.Server.Storage.Journaling;

internal readonly record struct JournalFrameReadResult(JournalFrameReadStatus Status, long FrameOffset, long NextFrameOffset);
