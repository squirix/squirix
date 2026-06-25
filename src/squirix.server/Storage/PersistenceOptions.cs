using System;
using System.Text.Json.Serialization;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Limits;

namespace Squirix.Server.Storage;

internal sealed record PersistenceOptions
{
    public PersistenceOptions()
    {
        FlushIntervalMs = 10;
        ManifestRetentionCount = 3;
        SnapshotRetentionCount = 3;
        JournalMaxSegmentMb = JournalSegmentLimits.DefaultMaxSegmentMb;
        JournalMaxSegmentCount = JournalSegmentLimits.DefaultMaxSegmentCount;
        JournalMaxTotalBytesMb = JournalSegmentLimits.DefaultMaxTotalBytesMb;
        JournalGroupCommitMaxBatch = 32;
        JournalGroupCommitMaxWait = TimeSpan.Zero;
        JournalWriteBatchBytes = JournalWriteBatchBuffer.DefaultCapacityBytes;
    }

    public string DataDir { get; init; } = string.Empty;

    public int FlushIntervalMs
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "FlushIntervalMs must be greater than zero.");

            field = value;
        }
    }

    /// <summary>Gets a value indicating whether journal group commit is enabled.</summary>
    public bool IsJournalGroupCommitEnabled => JournalGroupCommitMaxWait > TimeSpan.Zero;

    /// <summary>Gets the maximum number of concurrent durable mutations that can share one durability flush.</summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is not positive.</exception>
    [JsonPropertyName("groupCommitMaxBatch")]
    public int JournalGroupCommitMaxBatch
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalGroupCommitMaxBatch must be greater than zero.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the maximum time to wait for additional journal appends before issuing a shared durability flush.
    /// When zero, group commit is disabled and each durable mutation flushes independently.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    [JsonPropertyName("groupCommitMaxWait")]
    [JsonConverter(typeof(MillisecondsTimeSpanJsonConverter))]
    public TimeSpan JournalGroupCommitMaxWait
    {
        get;
        init
        {
            if (value < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalGroupCommitMaxWait cannot be negative.");

            field = value;
        }
    }

    [JsonPropertyName("journalMaxSegmentCount")]
    public int JournalMaxSegmentCount
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalMaxSegmentCount must be greater than zero.");

            field = value;
        }
    }

    [JsonPropertyName("journalMaxSegmentMb")]
    public int JournalMaxSegmentMb
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalMaxSegmentMb must be greater than zero.");

            field = value;
        }
    }

    [JsonPropertyName("journalMaxTotalBytesMb")]
    public int JournalMaxTotalBytesMb
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalMaxTotalBytesMb must be greater than zero.");

            field = value;
        }
    }

    [JsonPropertyName("journalPlatformBackend")]
    public JournalPlatformBackend JournalPlatformBackend { get; init; } = JournalPlatformBackend.Auto;

    /// <summary>
    /// Gets the size in bytes of the per-coordinator journal write-coalescing buffer. The buffer is
    /// allocated lazily on first staged append. Frames larger than this value bypass coalescing and
    /// are written directly.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is not positive.</exception>
    [JsonPropertyName("journalWriteBatchBytes")]
    public int JournalWriteBatchBytes
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalWriteBatchBytes must be greater than zero.");

            field = value;
        }
    }

    public int ManifestRetentionCount
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "ManifestRetentionCount must be greater than zero.");

            field = value;
        }
    }

    /// <summary>Gets the number of consecutive manifest writes with retention cleanup failures required to degrade readiness.</summary>
    public int RetentionCleanupDegradedConsecutiveWrites { get; init; } = 3;

    /// <summary>Gets the number of retention cleanup failures inside <see cref="RetentionCleanupDegradedWindowMinutes" /> required to degrade readiness.</summary>
    public int RetentionCleanupDegradedWindowFailures { get; init; } = 5;

    /// <summary>Gets the sliding window in minutes used when counting retention cleanup failures for readiness degradation.</summary>
    public int RetentionCleanupDegradedWindowMinutes { get; init; } = 15;

    public int SnapshotRetentionCount
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "SnapshotRetentionCount must be greater than zero.");

            field = value;
        }
    }
}
