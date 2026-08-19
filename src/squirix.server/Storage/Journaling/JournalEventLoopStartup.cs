using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

[Immutable]
internal sealed record JournalEventLoopStartup(int CurrentSegmentIndex, long JournalTotalBytes, int JournalSegmentCount);
