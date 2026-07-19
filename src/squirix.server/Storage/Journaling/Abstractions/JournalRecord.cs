using System;
using System.Collections.Concurrent;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Logical journal operation decoded from an on-disk frame (backend-neutral).</summary>
internal sealed class JournalRecord
{
    private static readonly ConcurrentBag<JournalRecord> AppendPool = [];

    /// <summary>Gets or sets idempotency operation id; only set for <see cref="JournalOperationKind.IdempotencyOutcome" />.</summary>
    internal string? IdempotencyOperationId { get; set; }

    /// <summary>Gets or sets idempotency response bytes; only set for <see cref="JournalOperationKind.IdempotencyOutcome" />.</summary>
    internal ReadOnlyMemory<byte> IdempotencyResponseBytes { get; set; }

    /// <summary>Gets or sets the cache key for the operation.</summary>
    internal CacheKey Key { get; set; } = CacheKey.Default(string.Empty);

    /// <summary>Gets or sets the journal operation kind.</summary>
    internal JournalOperationKind Operation { get; set; }

    /// <summary>Gets or sets put entry bytes; only set for <see cref="JournalOperationKind.Put" />.</summary>
    internal ReadOnlyMemory<byte> PutEntryBytes { get; set; }

    /// <summary>Gets or sets the monotonic journal sequence number.</summary>
    internal ulong Sequence { get; set; }

    /// <summary>Gets or sets touch expiration UTC; only set for <see cref="JournalOperationKind.TouchExpiration" />.</summary>
    internal DateTime? TouchExpirationUtc { get; set; }

    /// <summary>Gets or sets the operation timestamp in Unix milliseconds.</summary>
    internal long UnixMs { get; set; }

    /// <summary>Gets or sets idempotency fingerprint; only set for <see cref="JournalOperationKind.IdempotencyOutcome" />.</summary>
    internal string? IdempotencyFingerprint { get; set; }

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
