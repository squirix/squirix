using System;
using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage;

[Immutable]
internal sealed record PersistenceOptions
{
    /// <summary>Largest accepted group commit batch size.</summary>
    internal const int MaxGroupCommitBatch = 4096;

    /// <summary>Largest accepted group commit wait.</summary>
    private static readonly TimeSpan MaxGroupCommitWait = TimeSpan.FromMilliseconds(100);

    /// <summary>Smallest accepted positive group commit wait.</summary>
    private static readonly TimeSpan MinGroupCommitWait = TimeSpan.FromMilliseconds(1);

    /// <summary>Gets the root directory for durable storage, journal, snapshot, and manifest files.</summary>
    [JsonPropertyName("dataDir")]
    [JsonInclude]
    internal string DataDir { get; init; } = string.Empty;

    /// <summary>Gets a value indicating whether a journal group commit is enabled.</summary>
    internal bool IsJournalGroupCommitEnabled => JournalGroupCommitMaxWait > TimeSpan.Zero;

    /// <summary>Gets the maximum number of concurrent durable mutations that can share one durability flush.</summary>
    [JsonInclude]
    [JsonPropertyName("groupCommitMaxBatch")]
    internal int JournalGroupCommitMaxBatch { get; init; } = PersistenceOptionsDefaults.JournalGroupCommitMaxBatch;

    /// <summary>
    /// Gets the maximum time to wait for additional journal appends before issuing a shared durability flush.
    /// When zero, a group commit is disabled and each durable mutation flushes independently.
    /// </summary>
    [JsonPropertyName("journalGroupCommitMaxWait")]
    [JsonConverter(typeof(MillisecondsTimeSpanJsonConverter))]
    [JsonInclude]
    internal TimeSpan JournalGroupCommitMaxWait { get; init; } = PersistenceOptionsDefaults.JournalGroupCommitMaxWait;

    /// <summary>Gets the maximum number of journal segments to retain before compaction prunes older segments.</summary>
    [JsonPropertyName("journalMaxSegmentCount")]
    [JsonInclude]
    internal int JournalMaxSegmentCount { get; init; } = JournalSegmentLimits.DefaultMaxSegmentCount;

    /// <summary>Gets the maximum size of a single journal segment in megabytes.</summary>
    [JsonPropertyName("journalMaxSegmentMb")]
    [JsonInclude]
    internal int JournalMaxSegmentMb { get; init; } = JournalSegmentLimits.DefaultMaxSegmentMb;

    /// <summary>Gets the maximum total journal storage in megabytes across all segments.</summary>
    [JsonPropertyName("journalMaxTotalBytesMb")]
    [JsonInclude]
    internal int JournalMaxTotalBytesMb { get; init; } = JournalSegmentLimits.DefaultMaxTotalBytesMb;

    /// <summary>
    /// Gets how long one journal segment I/O call (a write or a flush) may stay in progress before readiness reports the node
    /// degraded. Internal host default only: not bound from configuration.
    /// </summary>
    internal TimeSpan JournalStallDegradedThreshold { get; init; } = PersistenceOptionsDefaults.JournalStallDegradedThreshold;

    /// <summary>
    /// Gets the size in bytes of the per-coordinator journal write-coalescing buffer. The buffer is allocated lazily on the first-staged appending.
    /// Frames larger than this value bypass coalescing and are written directly.
    /// </summary>
    [JsonPropertyName("journalWriteBatch")]
    [JsonInclude]
    internal int JournalWriteBatch { get; init; } = PersistenceOptionsDefaults.JournalWriteBatch;

    /// <summary>Gets the number of entries in the group log this node owns that triggers its compaction.</summary>
    [JsonPropertyName("replicaLogCompactionEntries")]
    [JsonInclude]
    internal int ReplicaLogCompactionEntries { get; init; } = PersistenceOptionsDefaults.ReplicaLogCompactionEntries;

    /// <summary>Gets the size in megabytes of the group log this node owns that triggers its compaction.</summary>
    [JsonPropertyName("replicaLogCompactionMb")]
    [JsonInclude]
    internal int ReplicaLogCompactionMb { get; init; } = PersistenceOptionsDefaults.ReplicaLogCompactionMb;

    /// <summary>Gets the number of manifest versions to retain before pruning older versions.</summary>
    [JsonPropertyName("manifestRetentionCount")]
    [JsonInclude]
    internal int ManifestRetentionCount { get; init; } = PersistenceOptionsDefaults.ManifestRetentionCount;

    /// <summary>Gets the number of retention cleanup failures inside <see cref="RetentionCleanupDegradedWindowMinutes" /> required to degrade readiness.</summary>
    [JsonPropertyName("retentionCleanupDegradedWindowFailures")]
    [JsonInclude]
    internal int RetentionCleanupDegradedWindowFailures { get; init; } = PersistenceOptionsDefaults.RetentionCleanupDegradedWindowFailures;

    /// <summary>Gets the sliding window in minutes used when counting retention cleanup failures for readiness degradation.</summary>
    [JsonPropertyName("retentionCleanupDegradedWindowMinutes")]
    [JsonInclude]
    internal int RetentionCleanupDegradedWindowMinutes { get; init; } = PersistenceOptionsDefaults.RetentionCleanupDegradedWindowMinutes;

    /// <summary>Gets the number of consecutive manifest writes with retention cleanup failures required to degrade readiness.</summary>
    [JsonPropertyName("retentionCleanupDegradedWrites")]
    [JsonInclude]
    internal int RetentionCleanupDegradedWrites { get; init; } = PersistenceOptionsDefaults.RetentionCleanupDegradedWrites;

    /// <summary>Gets the number of snapshots to retain before pruning older snapshots.</summary>
    [JsonPropertyName("snapshotRetentionCount")]
    [JsonInclude]
    internal int SnapshotRetentionCount { get; init; } = PersistenceOptionsDefaults.SnapshotRetentionCount;

    /// <summary>Validates scalar bounds; throws when any configured value is out of range.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a scalar is out of range.</exception>
    internal void Validate()
    {
        ValidateGroupCommit();

        RequirePositive(JournalMaxSegmentCount, nameof(JournalMaxSegmentCount));
        RequireHoldsLargestFrame(JournalMaxSegmentMb, nameof(JournalMaxSegmentMb));
        RequireHoldsLargestFrame(JournalMaxTotalBytesMb, nameof(JournalMaxTotalBytesMb));
        RequirePositive(JournalStallDegradedThreshold, nameof(JournalStallDegradedThreshold));
        RequirePositive(JournalWriteBatch, nameof(JournalWriteBatch));
        RequirePositive(ManifestRetentionCount, nameof(ManifestRetentionCount));
        RequirePositive(ReplicaLogCompactionEntries, nameof(ReplicaLogCompactionEntries));
        RequirePositive(ReplicaLogCompactionMb, nameof(ReplicaLogCompactionMb));
        RequirePositive(SnapshotRetentionCount, nameof(SnapshotRetentionCount));
    }

    /// <summary>Refuses a journal size that cannot hold the largest frame, under which valid writes would be refused forever.</summary>
    /// <param name="valueMb">The configured size in megabytes.</param>
    /// <param name="name">The option name.</param>
    /// <exception cref="InvalidOperationException">Thrown when the size is below <see cref="JournalSegmentLimits.MinSegmentMb" />.</exception>
    private static void RequireHoldsLargestFrame(int valueMb, string name)
    {
        if (valueMb < JournalSegmentLimits.MinSegmentMb)
        {
            throw new InvalidOperationException(
                $"Persistence {name} must be at least {JournalSegmentLimits.MinSegmentMb}: a journal segment must hold the largest journal frame.");
        }
    }

    private static void RequirePositive(int value, string name)
    {
        if (value <= 0)
            throw new InvalidOperationException($"Persistence {name} must be greater than zero.");
    }

    private static void RequirePositive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"Persistence {name} must be greater than zero.");
    }

    private void ValidateGroupCommit()
    {
        if (JournalGroupCommitMaxBatch is < 1 or > MaxGroupCommitBatch)
            throw new InvalidOperationException($"Journal GroupCommitMaxBatch must be between 1 and {MaxGroupCommitBatch}.");

        var wait = JournalGroupCommitMaxWait;
        if (wait == TimeSpan.Zero)
            return;

        if (wait < MinGroupCommitWait || wait > MaxGroupCommitWait || wait.Ticks % TimeSpan.TicksPerMillisecond != 0)
            throw new InvalidOperationException($"Journal GroupCommitMaxWait must be zero or between {MinGroupCommitWait.TotalMilliseconds} and {MaxGroupCommitWait.TotalMilliseconds} whole milliseconds (for example \"00:00:00.005\").");
    }

    private static class PersistenceOptionsDefaults
    {
        /// <summary>Default maximum number of concurrent durable mutations sharing one durability flush.</summary>
        internal const int JournalGroupCommitMaxBatch = 32;

        /// <summary>Default per-coordinator journal write-coalescing buffer size in bytes.</summary>
        internal const int JournalWriteBatch = 16 * 1024 * 1024;

        /// <summary>The default number of manifest versions retained before pruning.</summary>
        internal const int ManifestRetentionCount = 3;

        /// <summary>Default retention-cleanup failures inside the degradation window required to degrade readiness.</summary>
        internal const int RetentionCleanupDegradedWindowFailures = 5;

        /// <summary>Default sliding window in minutes for counting retention-cleanup failures.</summary>
        internal const int RetentionCleanupDegradedWindowMinutes = 15;

        /// <summary>Default number of owned group log entries that triggers a compaction.</summary>
        internal const int ReplicaLogCompactionEntries = 100_000;

        /// <summary>Default owned group log size in megabytes that triggers a compaction.</summary>
        internal const int ReplicaLogCompactionMb = 64;

        /// <summary>Default consecutive manifest retention-cleanup failures required to degrade readiness.</summary>
        internal const int RetentionCleanupDegradedWrites = 3;

        /// <summary>Default number of snapshots retained before pruning.</summary>
        internal const int SnapshotRetentionCount = 3;

        /// <summary>Default maximum wait for an additional journal appends before a shared durability flush; zero disables group commit.</summary>
        internal static readonly TimeSpan JournalGroupCommitMaxWait = TimeSpan.Zero;

        /// <summary>Default journal I/O stall duration before readiness reports degraded; the same order as the replication commit budget.</summary>
        internal static readonly TimeSpan JournalStallDegradedThreshold = TimeSpan.FromSeconds(5);
    }
}
