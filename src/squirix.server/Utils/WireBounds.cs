using System;
using System.Runtime.CompilerServices;

namespace Squirix.Server.Utils;

/// <summary>
/// Range checks for values a decoder reads from durable or wire bytes: lengths, Unix milliseconds and UTC ticks.
/// Each check is a predicate, so a decoder reports a value that fails it through its own contract (a <see langword="false" />
/// return, <see cref="System.IO.InvalidDataException" />, or <see langword="null" />) rather than through the exception a
/// framework conversion would throw. The checks stay inlineable in the caller.
/// </summary>
internal static class WireBounds
{
    /// <summary>Highest Unix millisecond value <see cref="DateTimeOffset" /> accepts.</summary>
    private static readonly long MaxUnixMilliseconds = DateTimeOffset.MaxValue.ToUnixTimeMilliseconds();

    /// <summary>Lowest Unix millisecond value <see cref="DateTimeOffset" /> accepts.</summary>
    private static readonly long MinUnixMilliseconds = DateTimeOffset.MinValue.ToUnixTimeMilliseconds();

    /// <summary>Returns whether <paramref name="length" /> bytes starting at <paramref name="offset" /> lie within a buffer.</summary>
    /// <remarks>
    /// The length is compared with the bytes left after the offset; no <c language="csharp">offset + length</c> sum is formed, so a
    /// decoded length near <see cref="int.MaxValue" /> cannot overflow past the check.
    /// </remarks>
    /// <param name="length">The decoded length.</param>
    /// <param name="offset">The offset the bytes start at.</param>
    /// <param name="bufferLength">The length of the buffer.</param>
    /// <returns><see langword="true" /> when the range is non-negative and ends at or before <paramref name="bufferLength" />.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool Fits(int length, int offset, int bufferLength) =>
        length >= 0 && offset >= 0 && offset <= bufferLength && length <= bufferLength - offset;

    /// <summary>Returns whether <paramref name="unixMilliseconds" /> names an instant <see cref="DateTimeOffset" /> can represent.</summary>
    /// <param name="unixMilliseconds">The decoded Unix milliseconds.</param>
    /// <returns><see langword="true" /> when the value converts without throwing.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsUnixMilliseconds(long unixMilliseconds) =>
        unixMilliseconds >= MinUnixMilliseconds && unixMilliseconds <= MaxUnixMilliseconds;

    /// <summary>Converts decoded Unix milliseconds to a UTC <see cref="DateTime" /> when they are in range.</summary>
    /// <param name="unixMilliseconds">The decoded Unix milliseconds.</param>
    /// <param name="utc">The UTC instant; the default value when the input is out of range.</param>
    /// <returns><see langword="true" /> when the value is in range.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool TryUtcFromUnixMilliseconds(long unixMilliseconds, out DateTime utc)
    {
        if (!IsUnixMilliseconds(unixMilliseconds))
        {
            utc = default;
            return false;
        }

        utc = DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds).UtcDateTime;
        return true;
    }

    /// <summary>Returns whether <paramref name="ticks" /> is a tick count <see cref="DateTime" /> accepts.</summary>
    /// <param name="ticks">The decoded ticks.</param>
    /// <returns><see langword="true" /> when the value is between zero and <see cref="DateTime.MaxValue" /> ticks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsUtcTicks(long ticks) => ticks >= 0 && ticks <= DateTime.MaxValue.Ticks;

    /// <summary>Converts decoded ticks to a UTC <see cref="DateTime" /> when they are in range.</summary>
    /// <param name="ticks">The decoded ticks.</param>
    /// <param name="utc">The UTC instant; the default value when the input is out of range.</param>
    /// <returns><see langword="true" /> when the value is in range.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool TryUtcFromTicks(long ticks, out DateTime utc)
    {
        if (!IsUtcTicks(ticks))
        {
            utc = default;
            return false;
        }

        utc = new DateTime(ticks, DateTimeKind.Utc);
        return true;
    }
}
