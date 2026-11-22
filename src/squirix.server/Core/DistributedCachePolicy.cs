using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Squirix.Server.Core;

/// <summary>The immutable expiration policy stored with a distributed cache payload.</summary>
[StructLayout(LayoutKind.Auto)]
internal readonly record struct DistributedCachePolicy
{
    /// <summary>The size of the v1 header in bytes.</summary>
    internal const int HeaderLength = 22;

    private const byte AbsoluteFlag = 1;
    private const byte SlidingFlag = 2;
    private const byte Version = 1;
    private const int AbsoluteOffset = 6;
    private const int SlidingOffset = 14;
    private const long MinUnixMilliseconds = -62135596800000L;
    private const long MaxUnixMilliseconds = 253402300799999L;

    /// <summary>Initializes a new instance of the <see cref="DistributedCachePolicy"/> struct.</summary>
    /// <param name="absoluteUnixMilliseconds">The inclusive UTC expiration cap, when present.</param>
    /// <param name="slidingTicks">The sliding interval in ticks, when present.</param>
    internal DistributedCachePolicy(long? absoluteUnixMilliseconds, long? slidingTicks)
    {
        AbsoluteUnixMilliseconds = absoluteUnixMilliseconds;
        SlidingTicks = slidingTicks;
    }

    /// <summary>Gets the inclusive UTC expiration cap, when present.</summary>
    internal long? AbsoluteUnixMilliseconds { get; }

    /// <summary>Gets the sliding interval in ticks, when present.</summary>
    internal long? SlidingTicks { get; }

    /// <summary>Reads and validates the policy without copying or interpreting the payload.</summary>
    /// <param name="bytes">The stored envelope.</param>
    /// <param name="policy">The validated policy on success.</param>
    /// <returns>Whether the header is a supported and consistent encoding.</returns>
    internal static bool TryRead(ReadOnlySpan<byte> bytes, out DistributedCachePolicy policy)
    {
        policy = default;
        if (bytes.Length < HeaderLength || !bytes[..4].SequenceEqual("SQDC"u8) || bytes[4] != Version)
            return false;

        var flags = bytes[5];
        if ((flags & ~(AbsoluteFlag | SlidingFlag)) != 0)
            return false;

        var absolute = BinaryPrimitives.ReadInt64LittleEndian(bytes[AbsoluteOffset..]);
        var sliding = BinaryPrimitives.ReadInt64LittleEndian(bytes[SlidingOffset..]);
        var hasAbsolute = (flags & AbsoluteFlag) != 0;
        var hasSliding = (flags & SlidingFlag) != 0;
        if ((!hasAbsolute && absolute != 0) || (!hasSliding && sliding != 0))
            return false;

        var decoded = new DistributedCachePolicy(hasAbsolute ? absolute : null, hasSliding ? sliding : null);
        if (!decoded.IsValid())
            return false;

        policy = decoded;
        return true;
    }

    /// <summary>Checks the normalized cap and sliding interval before storing a policy.</summary>
    /// <returns>Whether every present field is in its supported range.</returns>
    internal bool IsValid() =>
        AbsoluteUnixMilliseconds is not (< MinUnixMilliseconds or > MaxUnixMilliseconds) && SlidingTicks is not <= 0;

    /// <summary>Creates an owned v1 envelope for a validated policy and its payload.</summary>
    /// <param name="value">The caller payload.</param>
    /// <returns>The header followed by the original payload bytes.</returns>
    internal byte[] Encode(ReadOnlySpan<byte> value)
    {
        var bytes = GC.AllocateUninitializedArray<byte>(checked(HeaderLength + value.Length));
        bytes.AsSpan(0, HeaderLength).Clear();
        "SQDC"u8.CopyTo(bytes);
        bytes[4] = Version;
        byte flags = 0;
        if (AbsoluteUnixMilliseconds is { } absolute)
        {
            flags |= AbsoluteFlag;
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(AbsoluteOffset), absolute);
        }

        if (SlidingTicks is { } sliding)
        {
            flags |= SlidingFlag;
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(SlidingOffset), sliding);
        }

        bytes[5] = flags;
        value.CopyTo(bytes.AsSpan(HeaderLength));
        return bytes;
    }

    /// <summary>Chooses an initial deadline or renews a live sliding entry at the owner.</summary>
    /// <param name="nowUtc">The owner's current UTC time.</param>
    /// <param name="currentDeadline">The current deadline for a live entry being renewed, or null for a new value.</param>
    /// <param name="deadline">The chosen deadline, floored to the persisted millisecond precision.</param>
    /// <returns>Whether the chosen deadline is representable.</returns>
    internal bool TryChooseDeadline(DateTime nowUtc, DateTime? currentDeadline, out DateTime? deadline)
    {
        DateTime? cap = AbsoluteUnixMilliseconds is { } absolute ? DateTimeOffset.FromUnixTimeMilliseconds(absolute).UtcDateTime : null;
        deadline = cap;
        if (SlidingTicks is not { } sliding)
            return true;

        if (sliding > DateTime.MaxValue.Ticks - nowUtc.Ticks)
            return false;

        var ticks = Math.Max(nowUtc.Ticks + sliding, currentDeadline?.Ticks ?? 0);
        ticks = Math.Min(ticks, cap?.Ticks ?? DateTime.MaxValue.Ticks);
        deadline = new DateTime(ticks - (ticks % TimeSpan.TicksPerMillisecond), DateTimeKind.Utc);
        return true;
    }
}
