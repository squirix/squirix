namespace Squirix.Server.Storage.Journaling;

internal sealed record JournalEventLoopStartup(int CurrentSegmentIndex, long JournalTotalBytes, int JournalSegmentCount);
