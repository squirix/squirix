using System;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Logical journal operation decoded from an on-disk frame (backend-neutral).</summary>
internal sealed class JournalRecord
{
    public required ulong Sequence { get; init; }

    public required long UnixMs { get; init; }

    public required JournalOperationKind Operation { get; init; }

    public required CacheKey Key { get; init; }

    /// <summary>Gets put discriminated entry JSON bytes; only set for <see cref="JournalOperationKind.Put"/>.</summary>
    public byte[]? PutDiscriminatedEntryJson { get; init; }

    /// <summary>Gets put idempotency operation id; only set for <see cref="JournalOperationKind.Put"/>.</summary>
    public string? PutOperationId { get; init; }

    /// <summary>Gets touch expiration UTC; only set for <see cref="JournalOperationKind.TouchExpiration"/>.</summary>
    public DateTime? TouchExpirationUtc { get; init; }
}
