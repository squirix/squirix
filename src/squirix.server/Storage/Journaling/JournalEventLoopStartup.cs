using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Storage.Journaling;

[Immutable]
internal sealed record JournalEventLoopStartup(int CurrentSegmentIndex, long JournalTotalBytes, int JournalSegmentCount, JournalSegmentProbe ActiveSegment, JournalCapacityResyncSignal CapacityResync);
