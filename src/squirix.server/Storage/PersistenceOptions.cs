using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage;

[Immutable]
internal sealed record PersistenceOptions
{
    /// <summary>Gets the root directory for durable storage, journal, snapshot, and manifest files.</summary>
    [JsonPropertyName("dataDir")]
    [JsonInclude]
    internal string DataDir { get; init; } = string.Empty;

    /// <summary>Gets the persistence flush interval in milliseconds.</summary>
    [JsonPropertyName("flushInterval")]
    [JsonInclude]
    internal int FlushInterval { get; init; } = PersistenceOptionsDefaults.FlushInterval;

    /// <summary>Gets a value indicating whether journal group commit is enabled.</summary>
    internal bool IsJournalGroupCommitEnabled => JournalGroupCommitMaxWait > TimeSpan.Zero;

    /// <summary>Gets the maximum number of concurrent durable mutations that can share one durability flush.</summary>
    [JsonInclude]
    [JsonPropertyName("groupCommitMaxBatch")]
    internal int JournalGroupCommitMaxBatch { get; init; } = PersistenceOptionsDefaults.JournalGroupCommitMaxBatch;

    /// <summary>
    /// Gets the maximum time to wait for additional journal appends before issuing a shared durability flush.
    /// When zero, group commit is disabled and each durable mutation flushes independently.
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

    /// <summary>Gets the configured platform backend used for the journal storage.</summary>
    [JsonPropertyName("journalPlatformBackend")]
    [JsonInclude]
    internal JournalPlatformBackend JournalPlatformBackend { get; init; } = JournalPlatformBackend.Auto;

    /// <summary>
    /// Gets the size in bytes of the per-coordinator journal write-coalescing buffer. The buffer is
    /// allocated lazily on first staged append. Frames larger than this value bypass coalescing and
    /// are written directly.
    /// </summary>
    [JsonPropertyName("journalWriteBatch")]
    [JsonInclude]
    internal int JournalWriteBatch { get; init; } = PersistenceOptionsDefaults.JournalWriteBatch;

    /// <summary>Gets the number of manifest versions to retain before pruning older versions.</summary>
    [JsonPropertyName("manifestRetentionCount")]
    [JsonInclude]
    internal int ManifestRetentionCount { get; init; } = PersistenceOptionsDefaults.ManifestRetentionCount;

    /// <summary>Gets the number of consecutive manifest writes with retention cleanup failures required to degrade readiness.</summary>
    [JsonPropertyName("retentionCleanupDegradedWrites")]
    [JsonInclude]
    internal int RetentionCleanupDegradedWrites { get; init; } = PersistenceOptionsDefaults.RetentionCleanupDegradedWrites;

    /// <summary>Gets the number of retention cleanup failures inside <see cref="RetentionCleanupDegradedWindowMinutes" /> required to degrade readiness.</summary>
    [JsonPropertyName("retentionCleanupDegradedWindowFailures")]
    [JsonInclude]
    internal int RetentionCleanupDegradedWindowFailures { get; init; } = PersistenceOptionsDefaults.RetentionCleanupDegradedWindowFailures;

    /// <summary>Gets the sliding window in minutes used when counting retention cleanup failures for readiness degradation.</summary>
    [JsonPropertyName("retentionCleanupDegradedWindowMinutes")]
    [JsonInclude]
    internal int RetentionCleanupDegradedWindowMinutes { get; init; } = PersistenceOptionsDefaults.RetentionCleanupDegradedWindowMinutes;

    /// <summary>Gets the number of snapshots to retain before pruning older snapshots.</summary>
    [JsonPropertyName("snapshotRetentionCount")]
    [JsonInclude]
    internal int SnapshotRetentionCount { get; init; } = PersistenceOptionsDefaults.SnapshotRetentionCount;

    /// <summary>Validates scalar bounds; throws when any configured value is out of range.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a scalar is out of range.</exception>
    internal void Validate()
    {
        if (FlushInterval <= 0)
            throw new InvalidOperationException("Persistence FlushInterval must be greater than zero.");
        if (JournalGroupCommitMaxBatch <= 0)
            throw new InvalidOperationException("Persistence JournalGroupCommitMaxBatch must be greater than zero.");
        if (JournalGroupCommitMaxWait < TimeSpan.Zero)
            throw new InvalidOperationException("Persistence JournalGroupCommitMaxWait cannot be negative.");
        if (JournalMaxSegmentCount <= 0)
            throw new InvalidOperationException("Persistence JournalMaxSegmentCount must be greater than zero.");
        if (JournalMaxSegmentMb <= 0)
            throw new InvalidOperationException("Persistence JournalMaxSegmentMb must be greater than zero.");
        if (JournalMaxTotalBytesMb <= 0)
            throw new InvalidOperationException("Persistence JournalMaxTotalBytesMb must be greater than zero.");
        if (JournalWriteBatch <= 0)
            throw new InvalidOperationException("Persistence JournalWriteBatch must be greater than zero.");
        if (ManifestRetentionCount <= 0)
            throw new InvalidOperationException("Persistence ManifestRetentionCount must be greater than zero.");
        if (SnapshotRetentionCount <= 0)
            throw new InvalidOperationException("Persistence SnapshotRetentionCount must be greater than zero.");
    }

    private static class PersistenceOptionsDefaults
    {
        /// <summary>Default flush interval in milliseconds for the persistence pipeline.</summary>
        internal const int FlushInterval = 10;

        /// <summary>Default maximum number of concurrent durable mutations sharing one durability flush.</summary>
        internal const int JournalGroupCommitMaxBatch = 32;

        /// <summary>Default per-coordinator journal write-coalescing buffer size in bytes.</summary>
        internal const int JournalWriteBatch = 16 * 1024 * 1024;

        /// <summary>Default number of manifest versions retained before pruning.</summary>
        internal const int ManifestRetentionCount = 3;

        /// <summary>Default consecutive manifest retention-cleanup failures required to degrade readiness.</summary>
        internal const int RetentionCleanupDegradedWrites = 3;

        /// <summary>Default retention-cleanup failures inside the degradation window required to degrade readiness.</summary>
        internal const int RetentionCleanupDegradedWindowFailures = 5;

        /// <summary>Default sliding window in minutes for counting retention-cleanup failures.</summary>
        internal const int RetentionCleanupDegradedWindowMinutes = 15;

        /// <summary>Default number of snapshots retained before pruning.</summary>
        internal const int SnapshotRetentionCount = 3;

        /// <summary>Default maximum wait for additional journal appends before a shared durability flush; zero disables group commit.</summary>
        internal static readonly TimeSpan JournalGroupCommitMaxWait = TimeSpan.Zero;
    }

    private sealed class MillisecondsTimeSpanJsonConverter : JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType is JsonTokenType.Number && reader.TryGetInt64(out var milliseconds))
                return TimeSpan.FromMilliseconds(milliseconds);

            if (reader.TokenType != JsonTokenType.String)
                throw new JsonException("Expected a millisecond count or TimeSpan string.");
            var text = reader.GetString();
            if (text != null && TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var parsed))
                return parsed;

            throw new JsonException("Expected a millisecond count or TimeSpan string.");
        }

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options) => writer.WriteNumberValue(Convert.ToInt64(value.TotalMilliseconds));
    }
}
