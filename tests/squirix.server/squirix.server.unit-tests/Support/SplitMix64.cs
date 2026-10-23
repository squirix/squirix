using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Deterministic SplitMix64 generator, so a seeded test replays the same sequence on every run.</summary>
[Mutable]
internal sealed class SplitMix64
{
    private ulong _state;

    internal SplitMix64(ulong seed)
    {
        _state = seed;
    }

    /// <summary>Draws an integer below <paramref name="exclusiveMax" />.</summary>
    /// <param name="exclusiveMax">The bound; must be positive.</param>
    /// <returns>The drawn value.</returns>
    internal int Next(int exclusiveMax) => Convert.ToInt32(NextUInt64() % Convert.ToUInt64(exclusiveMax));

    /// <summary>Draws an integer in <paramref name="min" /> inclusive to <paramref name="exclusiveMax" /> exclusive.</summary>
    /// <param name="min">The smallest value.</param>
    /// <param name="exclusiveMax">The bound.</param>
    /// <returns>The drawn value.</returns>
    internal int Next(int min, int exclusiveMax) => min + Next(exclusiveMax - min);

    private ulong NextUInt64()
    {
        unchecked
        {
            _state += 0x9E3779B97F4A7C15UL;
            var z = _state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            return z ^ (z >> 31);
        }
    }
}
