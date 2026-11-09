using System;
using Grpc.Core;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>Carries a distributed Set request through admission until the owner pins its deadline.</summary>
/// <remarks>Only the envelope bytes and the pinned deadline are stored. The request object never reaches the cache or the journal.</remarks>
[Immutable]
internal sealed class DistributedCacheWrite
{
    private readonly DistributedCachePolicy _policy;

    /// <summary>Initializes a new instance of the <see cref="DistributedCacheWrite"/> class with an owned envelope.</summary>
    /// <param name="policy">The validated request policy.</param>
    /// <param name="payload">The value to store.</param>
    internal DistributedCacheWrite(in DistributedCachePolicy policy, ReadOnlySpan<byte> payload)
    {
        _policy = policy;
        Envelope = policy.Encode(payload);
    }

    /// <summary>Gets the owned, immutable envelope.</summary>
    internal byte[] Envelope { get; }

    /// <summary>Gets a value indicating whether a future read can both rewrite the entry and retain its payload as an outcome.</summary>
    internal bool HasSlidingExpiration => _policy.SlidingTicks != null;

    /// <summary>Gets a marker for entry size estimates when the write will have a deadline.</summary>
    internal DateTime? DeadlineForSizing => _policy.AbsoluteUnixMilliseconds != null || _policy.SlidingTicks != null ? DateTime.UnixEpoch : null;

    /// <summary>Resolves a Set to the envelope bytes and one absolute deadline.</summary>
    /// <typeparam name="T">The server cache value type.</typeparam>
    /// <param name="now">The owner clock at preparation.</param>
    /// <returns>The entry ready for storage.</returns>
    /// <exception cref="RpcException">The requested deadline cannot be represented.</exception>
    internal NodeCacheEntry<T> Materialize<T>(DateTime now)
    {
        if (!_policy.TryChooseDeadline(now, null, out var deadline) || Envelope is not T value)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "The distributed cache policy cannot be materialized."));

        // The replica log reserves zero ticks for no expiry. An older cap still replaces the value with an expired entry.
        if (deadline is { Ticks: 0 })
        {
            var earliest = DateTime.MinValue.AddMilliseconds(1);
            if (now < earliest)
                throw new RpcException(new Status(StatusCode.InvalidArgument, "The distributed cache deadline cannot be represented."));

            deadline = DateTime.SpecifyKind(earliest, DateTimeKind.Utc);
        }

        return new NodeCacheEntry<T>(value, expiresUtc: deadline);
    }
}
