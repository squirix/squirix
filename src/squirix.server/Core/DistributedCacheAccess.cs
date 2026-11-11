using System;
using Grpc.Core;

namespace Squirix.Server.Core;

/// <summary>Decides the visibility and renewal of a stored distributed value under the owner's write serialization.</summary>
internal static class DistributedCacheAccess
{
    /// <summary>Reads the immutable policy of a live entry and computes its replacement deadline, if any.</summary>
    /// <typeparam name="T">The type used by the node to hold values.</typeparam>
    /// <param name="current">The entry observed in the serialized decision.</param>
    /// <param name="now">The owner clock at the decision point.</param>
    /// <returns>The payload when live, the new deadline when it changes, and whether a stored entry expired.</returns>
    /// <exception cref="RpcException">The live entry has a malformed envelope or an unrepresentable deadline.</exception>
    internal static (ReadOnlyMemory<byte>? Payload, DateTime? RenewTo, bool Expired) Decide<T>(NodeCacheEntry<T>? current, DateTime now)
    {
        if (current == null)
            return default;

        if (current.ExpiresUtc is { } expires && expires <= now)
            return (null, null, true);

        var payload = ReadPayload(current.Value, out var policy);
        if (policy.SlidingTicks == null)
            return (payload, null, false);

        if (current.ExpiresUtc == null || !policy.TryChooseDeadline(now, current.ExpiresUtc, out var deadline))
            throw new RpcException(new Status(StatusCode.DataLoss, "The distributed cache entry has an invalid sliding deadline."));
        var renewal = deadline > current.ExpiresUtc ? deadline : null;
        return (payload, renewal, false);
    }

    /// <summary>Reads the bytes and policy of an adapter entry without copying the payload.</summary>
    /// <param name="value">The stored value.</param>
    /// <param name="policy">The policy read from the envelope.</param>
    /// <returns>The payload, including a possible empty value.</returns>
    /// <exception cref="RpcException">The entry is not a valid adapter envelope.</exception>
    internal static ReadOnlyMemory<byte> ReadPayload(object? value, out DistributedCachePolicy policy) =>
        value is byte[] bytes && DistributedCachePolicy.TryRead(bytes, out policy)
            ? bytes.AsMemory(DistributedCachePolicy.HeaderLength)
            : throw new RpcException(new Status(StatusCode.DataLoss, "The distributed cache entry has an invalid policy envelope."));
}
