using System.Text.Json.Serialization;
using Squirix.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed class HealthReadyDetailsResponse
{
    internal HealthReadyDetailsResponse(ulong journalBacklogOps, double? snapshotAgeSeconds, bool snapshotInFlight, HealthReadyDetailSections sections)
    {
        JournalBacklogOps = journalBacklogOps;
        SnapshotAgeSeconds = snapshotAgeSeconds;
        SnapshotInFlight = snapshotInFlight;
        Compaction = sections.Compaction;
        ClientPool = sections.ClientPool;
        Coordination = sections.Coordination;
        MemoryPressure = sections.MemoryPressure;
        RetentionCleanup = sections.RetentionCleanup;
        JournalDisk = sections.JournalDisk;
    }

    [JsonInclude]
    internal HealthClientPoolDetails ClientPool { get; }

    [JsonInclude]
    internal HealthCompactionDetails Compaction { get; }

    [JsonInclude]
    internal HealthCoordinationDetails Coordination { get; }

    [JsonInclude]
    [JsonPropertyName("journalBacklogOps")]
    internal ulong JournalBacklogOps { get; }

    [JsonInclude]
    internal HealthJournalDiskDetails JournalDisk { get; }

    [JsonInclude]
    internal HealthMemoryPressureDetails MemoryPressure { get; }

    [JsonInclude]
    internal HealthRetentionCleanupDetails RetentionCleanup { get; }

    [JsonInclude]
    internal double? SnapshotAgeSeconds { get; }

    [JsonInclude]
    internal bool SnapshotInFlight { get; }
}
