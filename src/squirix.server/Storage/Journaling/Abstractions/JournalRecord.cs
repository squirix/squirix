using System;
using System.Collections.Concurrent;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.JsonFramed;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Logical journal operation decoded from an on-disk frame (backend-neutral).</summary>
internal sealed class JournalRecord
{
    private static readonly ConcurrentBag<JournalRecord> AppendPool = [];

    /// <summary>Gets or sets the monotonic journal sequence number.</summary>
    public ulong Sequence { get; set; }

    /// <summary>Gets or sets the operation timestamp in Unix milliseconds.</summary>
    public long UnixMs { get; set; }

    /// <summary>Gets or sets the journal operation kind.</summary>
    public JournalOperationKind Operation { get; set; }

    /// <summary>Gets or sets the cache key for the operation.</summary>
    public CacheKey Key { get; set; }

    /// <summary>Gets or sets put discriminated entry JSON bytes; only set for <see cref="JournalOperationKind.Put"/>.</summary>
    public byte[]? PutDiscriminatedEntryJson { get; set; }

    /// <summary>Gets or sets put idempotency operation id; only set for <see cref="JournalOperationKind.Put"/>.</summary>
    public string? PutOperationId { get; set; }

    /// <summary>Gets or sets touch expiration UTC; only set for <see cref="JournalOperationKind.TouchExpiration"/>.</summary>
    public DateTime? TouchExpirationUtc { get; set; }

    internal static JournalRecord RentForAppend() => AppendPool.TryTake(out var record) ? record : new JournalRecord();

    internal void ReturnToAppendPool()
    {
        PutDiscriminatedEntryJson = null;
        PutOperationId = null;
        TouchExpirationUtc = null;
        AppendPool.Add(this);
    }
}
