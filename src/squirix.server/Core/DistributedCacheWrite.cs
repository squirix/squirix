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
    /// <param name="ttlTicks">The optional relative cap, resolved at owner preparation.</param>
    internal DistributedCacheWrite(in DistributedCachePolicy policy, ReadOnlySpan<byte> payload, long? ttlTicks = null)
    {
        _policy = policy;
        TtlTicks = ttlTicks;
        Envelope = policy.Encode(payload);
    }

    /// <summary>Gets a marker for entry size estimates when the write will have a deadline.</summary>
    internal DateTime? DeadlineForSizing => TtlTicks != null || _policy.AbsoluteUnixMilliseconds != null || _policy.SlidingTicks != null ? DateTime.UnixEpoch : null;

    /// <summary>Gets the owned, immutable envelope.</summary>
    internal byte[] Envelope { get; }

    /// <summary>Gets a value indicating whether a future read can both rewrite the entry and retain its payload as an outcome.</summary>
    internal bool HasSlidingExpiration => _policy.SlidingTicks != null;

    /// <summary>Gets the relative expiration cap requested by the client, in ticks.</summary>
    internal long? TtlTicks { get; }

    /// <summary>Resolves a Set to the envelope bytes and one absolute deadline.</summary>
    /// <typeparam name="T">The server cache value type.</typeparam>
    /// <param name="now">The owner clock at preparation.</param>
    /// <returns>The entry ready for storage.</returns>
    /// <exception cref="RpcException">The requested deadline cannot be represented.</exception>
    internal NodeCacheEntry<T> Materialize<T>(DateTime now)
    {
        var policy = _policy;
        if (TtlTicks is { } ttl)
        {
            if (ttl <= 0 || ttl > DateTime.MaxValue.Ticks - now.Ticks)
                throw InvalidPolicy();

            var relativeCap = new DateTimeOffset(now.AddTicks(ttl), TimeSpan.Zero).ToUnixTimeMilliseconds();
            var cap = Math.Min(relativeCap, policy.AbsoluteUnixMilliseconds ?? relativeCap);
            policy = new DistributedCachePolicy(cap, policy.SlidingTicks);
        }

        if (!policy.TryChooseDeadline(now, null, out var deadline))
            throw InvalidPolicy();

        // Only sliding renewal needs the resolved cap in the stored envelope.
        var envelope = HasSlidingExpiration && policy != _policy
            ? policy.Encode(Envelope.AsSpan(DistributedCachePolicy.HeaderLength))
            : Envelope;
        if (envelope is not T value)
            throw InvalidPolicy();

        // The replica log reserves zero ticks for no expiry. An older cap still replaces the value with an expired entry.
        if (deadline is not { Ticks: 0 })
            return new NodeCacheEntry<T>(value, expiresUtc: deadline);
        var earliest = DateTime.MinValue.AddMilliseconds(1);
        if (now < earliest)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "The distributed cache deadline cannot be represented."));

        deadline = DateTime.SpecifyKind(earliest, DateTimeKind.Utc);
        return new NodeCacheEntry<T>(value, expiresUtc: deadline);
    }

    private static RpcException InvalidPolicy() => new(new Status(StatusCode.InvalidArgument, "The distributed cache policy cannot be materialized."));
}
