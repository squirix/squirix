using System;
using System.Collections.Concurrent;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Observability;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Logical journal operation decoded from an on-disk frame (backend-neutral).</summary>
internal sealed class JournalRecord
{
    private static readonly ConcurrentBag<JournalRecord> AppendPool = [];

    /// <summary>Gets or sets idempotency fingerprint; only set for <see cref="JournalOperationKind.IdempotencyOutcome" />.</summary>
    public string? IdempotencyFingerprint { get; set; }

    /// <summary>Gets or sets idempotency operation id; only set for <see cref="JournalOperationKind.IdempotencyOutcome" />.</summary>
    public string? IdempotencyOperationId { get; set; }

    /// <summary>Gets or sets idempotency response bytes; only set for <see cref="JournalOperationKind.IdempotencyOutcome" />.</summary>
    public ReadOnlyMemory<byte> IdempotencyResponseBytes { get; set; }

    /// <summary>Gets or sets the cache key for the operation.</summary>
    public CacheKey Key { get; set; }

    /// <summary>Gets or sets the journal operation kind.</summary>
    public JournalOperationKind Operation { get; set; }

    /// <summary>Gets or sets put entry bytes; only set for <see cref="JournalOperationKind.Put" />.</summary>
    public ReadOnlyMemory<byte> PutEntryBytes { get; set; }

    /// <summary>Gets or sets the monotonic journal sequence number.</summary>
    public ulong Sequence { get; set; }

    /// <summary>Gets or sets touch expiration UTC; only set for <see cref="JournalOperationKind.TouchExpiration" />.</summary>
    public DateTime? TouchExpirationUtc { get; set; }

    /// <summary>Gets or sets the operation timestamp in Unix milliseconds.</summary>
    public long UnixMs { get; set; }

    internal static JournalRecord RentForAppend() => AppendPool.TryTake(out var record) ? record : new JournalRecord();

    internal void ReturnToAppendPool()
    {
        PutEntryBytes = default;
        TouchExpirationUtc = null;
        IdempotencyOperationId = null;
        IdempotencyFingerprint = null;
        IdempotencyResponseBytes = default;
        AppendPool.Add(this);
    }
}
