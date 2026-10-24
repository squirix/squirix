using System;
using Google.Protobuf.WellKnownTypes;

namespace Squirix.Transport.Grpc;

/// <summary>
/// Converts between <see cref="TimeSpan" /> and the protobuf <see cref="Duration" /> without range failures: a relative expiration beyond the
/// normalized <see cref="Duration" /> range means "practically never expires" and saturates at the range limit instead of failing the call.
/// </summary>
internal static class WireDuration
{
    /// <summary>The largest magnitude, in seconds, of a normalized <see cref="Duration" /> (about 10,000 years).</summary>
    internal const long MaxSeconds = 315_576_000_000L;

    private const int NanosPerTick = 100;

    private static readonly TimeSpan MaxTimeSpan = new(MaxSeconds * TimeSpan.TicksPerSecond);

    /// <summary>Converts <paramref name="value" /> to a <see cref="Duration" />, clamping to the normalized range.</summary>
    /// <param name="value">The time span to send.</param>
    /// <returns>A normalized duration; magnitudes beyond <see cref="MaxSeconds" /> saturate at the limit.</returns>
    internal static Duration FromTimeSpan(TimeSpan value) => Duration.FromTimeSpan(new TimeSpan(Math.Clamp(value.Ticks, -MaxTimeSpan.Ticks, MaxTimeSpan.Ticks)));

    /// <summary>Converts <paramref name="value" /> to a <see cref="TimeSpan" /> without throwing for out-of-range or non-normalized input.</summary>
    /// <param name="value">The wire duration, possibly produced by a foreign client.</param>
    /// <returns>The time span; a positive magnitude beyond <see cref="MaxSeconds" /> saturates to <see cref="TimeSpan.MaxValue" />, a negative one to the negated limit.</returns>
    internal static TimeSpan ToTimeSpan(Duration value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return value.Seconds > MaxSeconds
            ? TimeSpan.MaxValue
            : new TimeSpan((Math.Max(value.Seconds, -MaxSeconds) * TimeSpan.TicksPerSecond) + (value.Nanos / NanosPerTick));
    }
}
