using System;

namespace Squirix.Server.Utils;

/// <summary>Overflow-safe arithmetic helpers for <see cref="DateTime" />.</summary>
internal static class DateTimeEx
{
    /// <summary>
    /// Adds <paramref name="delta" /> to <paramref name="value" />, saturating at the
    /// <see cref="DateTime.MinValue" /> and <see cref="DateTime.MaxValue" /> boundaries instead of
    /// throwing <see cref="ArgumentOutOfRangeException" />.
    /// </summary>
    /// <param name="value">The value to add onto.</param>
    /// <param name="delta">The offset to add.</param>
    /// <returns>The saturated result.</returns>
    internal static DateTime SaturatedAdd(this DateTime value, TimeSpan delta)
    {
        if (delta > TimeSpan.Zero && delta > DateTime.MaxValue - value)
            return DateTime.MaxValue;

        if (delta < TimeSpan.Zero && delta < DateTime.MinValue - value)
            return DateTime.MinValue;

        return value.Add(delta);
    }
}
