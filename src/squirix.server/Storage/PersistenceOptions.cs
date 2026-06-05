using System;
using System.Text.Json.Serialization;

namespace Squirix.Server.Storage;

internal sealed record PersistenceOptions
{
    public PersistenceOptions()
    {
        FlushIntervalMs = 10;
        ManifestRetentionCount = 3;
        SnapshotIntervalSec = 60;
        SnapshotRetentionCount = 3;
        JournalMaxSegmentMb = 128;
        JournalGroupCommitMaxBatch = 32;
    }

    public string DataDir { get; init; } = string.Empty;

    public int FlushIntervalMs
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(FlushIntervalMs), value, "FlushIntervalMs must be greater than zero.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the maximum number of concurrent durable mutations that can share one durability flush.
    /// </summary>
    [JsonPropertyName("groupCommitMaxBatch")]
    public int JournalGroupCommitMaxBatch
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(JournalGroupCommitMaxBatch), value, "JournalGroupCommitMaxBatch must be greater than zero.");

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
                throw new ArgumentOutOfRangeException(nameof(JournalGroupCommitMaxWaitMs), value, "JournalGroupCommitMaxWaitMs cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets a value indicating whether journal group commit is enabled.
    /// </summary>
    public bool IsJournalGroupCommitEnabled => JournalGroupCommitMaxWaitMs > 0;

    public int ManifestRetentionCount
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(ManifestRetentionCount), value, "ManifestRetentionCount must be greater than zero.");

            field = value;
        }
    }

    public int SnapshotIntervalSec
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(SnapshotIntervalSec), value, "SnapshotIntervalSec must be greater than zero.");

            field = value;
        }
    }

    public int SnapshotRetentionCount
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(SnapshotRetentionCount), value, "SnapshotRetentionCount must be greater than zero.");

            field = value;
        }
    }

    public bool StrictFsync { get; init; }

    [JsonPropertyName("journalMaxSegmentMb")]
    public int JournalMaxSegmentMb
    {
        get;
        init
        {
            if (value <= 0)
                throw new ArgumentOutOfRangeException(nameof(JournalMaxSegmentMb), value, "JournalMaxSegmentMb must be greater than zero.");

            field = value;
        }
    }
}
