namespace Squirix.Server.Storage.Journaling.Read;

internal sealed record JournalFrameReadResult(JournalFrameReadStatus Status, long NextFrameOffset);
