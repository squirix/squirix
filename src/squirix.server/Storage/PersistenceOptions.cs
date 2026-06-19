using System;
using System.Text.Json.Serialization;
using Squirix.Server.Storage.Journaling.PipelinedWal;
using Squirix.Server.Storage.Journaling.PipelinedWal.Limits;

namespace Squirix.Server.Storage;

internal sealed record PersistenceOptions
{
    public PersistenceOptions()
    {
        FlushIntervalMs = 10;
        ManifestRetentionCount = 3;
        SnapshotIntervalSec = 60;
        SnapshotRetentionCount = 3;
        JournalMaxSegmentMb = WalSegmentLimits.DefaultMaxSegmentMb;
        JournalMaxSegmentCount = WalSegmentLimits.DefaultMaxSegmentCount;
        JournalMaxTotalBytesMb = WalSegmentLimits.DefaultMaxTotalBytesMb;
        JournalGroupCommitMaxBatch = 32;
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
    public bool IsJournalGroupCommitEnabled => JournalGroupCommitMaxWaitMs > 0;

    /// <summary>Gets the maximum number of concurrent durable mutations that can share one durability flush.</summary>
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
    /// Gets the maximum time in milliseconds to wait for additional journal appends before issuing a shared durability flush.
    /// When zero, group commit is disabled and each durable mutation flushes independently.
    /// </summary>
    [JsonPropertyName("groupCommitMaxWaitMs")]
    public int JournalGroupCommitMaxWaitMs
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalGroupCommitMaxWaitMs cannot be negative.");

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

    [JsonPropertyName("journalBackend")]
    public JournalBackend JournalBackend { get; init; } = JournalBackend.PipelinedWal;

    [JsonPropertyName("walPlatformBackend")]
    public WalPlatformBackend WalPlatformBackend { get; init; } = WalPlatformBackend.Auto;

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

    public int SnapshotIntervalSec
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "SnapshotIntervalSec must be greater than zero.");

            field = value;
        }
    }

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

    /// <summary>Gets the number of consecutive manifest writes with retention cleanup failures required to degrade readiness.</summary>
    public int RetentionCleanupDegradedConsecutiveWrites { get; init; } = 3;

    /// <summary>Gets the sliding window in minutes used when counting retention cleanup failures for readiness degradation.</summary>
    public int RetentionCleanupDegradedWindowMinutes { get; init; } = 15;

    /// <summary>
    /// Gets the number of retention cleanup failures inside <see cref="RetentionCleanupDegradedWindowMinutes" /> required to degrade readiness.
    /// </summary>
    public int RetentionCleanupDegradedWindowFailures { get; init; } = 5;
}
