namespace Squirix.Server.Storage.Journaling;

internal sealed record JournalFrameReadResult(JournalFrameReadStatus Status, long NextFrameOffset);
