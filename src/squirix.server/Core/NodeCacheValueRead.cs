using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>A value read that also carries the deadline of the stored entry, for a caller that judges expiry itself without the whole entry.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <remarks>
/// The hit flag and the deadline share one field, so the read is 16 bytes: the value and the state. A read through the awaits of the cache
/// layers measured about 15 ns faster at this size than as a 32-byte struct with a separate flag and nullable deadline. The default value is a miss.
/// </remarks>
[Immutable]
internal readonly record struct NodeCacheValueRead<T>
{
    /// <summary>The state of a miss: the default value of the struct.</summary>
    private const long MissState = 0;

    /// <summary>The state of a hit whose entry has no deadline.</summary>
    private const long NoDeadlineState = 1;

    /// <summary>What the state of a hit with a deadline adds to the deadline ticks, past the two reserved states.</summary>
    private const long DeadlineStateOffset = 2;

    private readonly long _state;

    /// <summary>Initializes a new instance of the <see cref="NodeCacheValueRead{T}" /> struct.</summary>
    /// <param name="found">Whether the key was present.</param>
    /// <param name="value">The stored value (may be <see langword="null" />); ignored for a miss.</param>
    /// <param name="expiresUtc">The absolute UTC deadline of the stored entry, or <see langword="null" /> for none; ignored for a miss.</param>
    internal NodeCacheValueRead(bool found, T? value, DateTime? expiresUtc)
    {
        if (!found)
            return;

        Value = value;
        _state = expiresUtc is { } deadline ? deadline.Ticks + DeadlineStateOffset : NoDeadlineState;
    }

    /// <summary>Gets a value indicating whether the key was present.</summary>
    internal bool Found => _state != MissState;

    /// <summary>Gets the stored value (may be <see langword="null" />); the default for a miss.</summary>
    internal T? Value { get; }

    /// <summary>Gets the absolute UTC deadline of the stored entry, or <see langword="null" /> for none or for a miss.</summary>
    internal DateTime? ExpiresUtc => _state >= DeadlineStateOffset ? new DateTime(_state - DeadlineStateOffset, DateTimeKind.Utc) : null;
}
