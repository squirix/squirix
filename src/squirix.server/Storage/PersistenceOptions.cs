using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix.Server.Storage;

internal sealed record PersistenceOptions
{
    [JsonInclude]
    internal string DataDir { get; init; } = string.Empty;

    [JsonInclude]
    internal int FlushIntervalMs { get; init; } = 10;

    /// <summary>Gets a value indicating whether journal group commit is enabled.</summary>
    internal bool IsJournalGroupCommitEnabled => JournalGroupCommitMaxWait > TimeSpan.Zero;

    /// <summary>Gets the maximum number of concurrent durable mutations that can share one durability flush.</summary>
    [JsonInclude]
    [JsonPropertyName("groupCommitMaxBatch")]
    internal int JournalGroupCommitMaxBatch { get; init; } = 32;

    /// <summary>
    /// Gets the maximum time to wait for additional journal appends before issuing a shared durability flush.
    /// When zero, group commit is disabled and each durable mutation flushes independently.
    /// </summary>
    [JsonPropertyName("groupCommitMaxWait")]
    [JsonConverter(typeof(MillisecondsTimeSpanJsonConverter))]
    [JsonInclude]
    internal TimeSpan JournalGroupCommitMaxWait { get; init; } = TimeSpan.Zero;

    [JsonPropertyName("journalMaxSegmentCount")]
    [JsonInclude]
    internal int JournalMaxSegmentCount { get; init; } = JournalSegmentLimits.DefaultMaxSegmentCount;

    [JsonPropertyName("journalMaxSegmentMb")]
    [JsonInclude]
    internal int JournalMaxSegmentMb { get; init; } = JournalSegmentLimits.DefaultMaxSegmentMb;

    [JsonPropertyName("journalMaxTotalBytesMb")]
    [JsonInclude]
    internal int JournalMaxTotalBytesMb { get; init; } = JournalSegmentLimits.DefaultMaxTotalBytesMb;

    [JsonPropertyName("journalPlatformBackend")]
    [JsonInclude]
    internal JournalPlatformBackend JournalPlatformBackend { get; init; } = JournalPlatformBackend.Auto;

    /// <summary>
    /// Gets the size in bytes of the per-coordinator journal write-coalescing buffer. The buffer is
    /// allocated lazily on first staged append. Frames larger than this value bypass coalescing and
    /// are written directly.
    /// </summary>
    [JsonPropertyName("journalWriteBatchBytes")]
    [JsonInclude]
    internal int JournalWriteBatchBytes { get; init; } = 16 * 1024 * 1024;

    [JsonInclude]
    internal int ManifestRetentionCount { get; init; } = 3;

    /// <summary>Gets the number of consecutive manifest writes with retention cleanup failures required to degrade readiness.</summary>
    [JsonInclude]
    internal int RetentionCleanupDegradedConsecutiveWrites { get; init; } = 3;

    /// <summary>Gets the number of retention cleanup failures inside <see cref="RetentionCleanupDegradedWindowMinutes" /> required to degrade readiness.</summary>
    [JsonInclude]
    internal int RetentionCleanupDegradedWindowFailures { get; init; } = 5;

    /// <summary>Gets the sliding window in minutes used when counting retention cleanup failures for readiness degradation.</summary>
    [JsonInclude]
    internal int RetentionCleanupDegradedWindowMinutes { get; init; } = 15;

    [JsonInclude]
    internal int SnapshotRetentionCount { get; init; } = 3;

    /// <summary>Validates scalar bounds; throws when any configured value is out of range.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a scalar is out of range.</exception>
    internal void Validate()
    {
        if (FlushIntervalMs <= 0)
            throw new InvalidOperationException("Persistence FlushIntervalMs must be greater than zero.");
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
        if (JournalWriteBatchBytes <= 0)
            throw new InvalidOperationException("Persistence JournalWriteBatchBytes must be greater than zero.");
        if (ManifestRetentionCount <= 0)
            throw new InvalidOperationException("Persistence ManifestRetentionCount must be greater than zero.");
        if (SnapshotRetentionCount <= 0)
            throw new InvalidOperationException("Persistence SnapshotRetentionCount must be greater than zero.");
    }

    private sealed class MillisecondsTimeSpanJsonConverter : JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType is JsonTokenType.Number && reader.TryGetInt64(out var milliseconds))
                return TimeSpan.FromMilliseconds(milliseconds);

            if (reader.TokenType is not JsonTokenType.String)
                throw new JsonException("Expected a millisecond count or TimeSpan string.");
            var text = reader.GetString();
            if (text is not null && TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var parsed))
                return parsed;

            throw new JsonException("Expected a millisecond count or TimeSpan string.");
        }

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options) => writer.WriteNumberValue(Convert.ToInt64(value.TotalMilliseconds));
    }
}
