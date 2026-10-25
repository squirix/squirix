using System;
using Google.Protobuf.WellKnownTypes;

namespace Squirix.Transport.Grpc;

/// <summary>Validates protobuf <see cref="Timestamp" /> values received from the wire before they are converted to CLR types.</summary>
internal static class WireTimestamp
{
    /// <summary>The smallest valid <see cref="Timestamp.Seconds" />: 0001-01-01T00:00:00Z.</summary>
    internal const long MinSeconds = -62_135_596_800L;

    /// <summary>The largest valid <see cref="Timestamp.Seconds" />: 9999-12-31T23:59:59Z.</summary>
    internal const long MaxSeconds = 253_402_300_799L;

    /// <summary>The largest valid <see cref="Timestamp.Nanos" /> value.</summary>
    internal const int MaxNanos = 999_999_999;

    /// <summary>Determines whether <paramref name="value" /> is a normalized timestamp that converts to a <see cref="DateTime" /> without failing.</summary>
    /// <param name="value">The wire timestamp.</param>
    /// <returns><see langword="true" /> when nanos are within 0..999,999,999 and seconds are within 0001-01-01..9999-12-31.</returns>
    internal static bool IsNormalized(Timestamp value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return value.Nanos is >= 0 and <= MaxNanos && value.Seconds is >= MinSeconds and <= MaxSeconds;
    }
}
