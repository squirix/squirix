namespace Squirix.Server.LocalCache;

/// <summary>Maintains positive access counts for frequency-based eviction.</summary>
internal static class CacheAccessFrequency
{
    /// <summary>Increments an access count without wrapping below the minimum frequency.</summary>
    /// <param name="frequency">The current access count.</param>
    /// <returns>The next count, saturated at the maximum value.</returns>
    internal static long Increment(long frequency) => frequency < long.MaxValue ? frequency + 1 : frequency;
}
