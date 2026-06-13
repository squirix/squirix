using System;
using System.Collections.Generic;

namespace Squirix.Server.PropertyTests;

/// <summary>
/// Miscellaneous helpers for property-based tests around the consistent hashing ring.
/// Provides deterministic key generation and stable shuffling utilities used in sampling-heavy checks.
/// </summary>
internal static class RingHelpers
{
    /// <summary>
    /// Generates a deterministic sequence of pseudo-random keys suitable for large sampling loops.
    /// </summary>
    /// <param name="count">Number of keys to generate. Must be non-negative.</param>
    /// <param name="seed">Seed for the pseudo-random generator to ensure reproducibility.</param>
    /// <returns>
    /// A sequence of unique (with high probability) string keys based on a seeded PRNG.
    /// </returns>
    /// <remarks>
    /// Keys are produced in the form <c>"key-{NextInt64()}"</c>. Using a fixed <paramref name="seed" />
    /// guarantees the same sequence across runs, which is important for reproducible property failures.
    /// </remarks>
    public static IEnumerable<string> MakeKeys(int count, int seed)
    {
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be non-negative.");

        var rng = new DeterministicRandom(seed);
        for (var i = 0; i < count; i++)
            yield return $"key-{rng.NextInt64()}";
    }

    /// <summary>
    /// Returns a shuffled copy of <paramref name="source" /> using the Fisher–Yates algorithm.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Input sequence to shuffle. Must not be <c>null</c>.</param>
    /// <param name="seed">Seed for the pseudo-random generator to ensure reproducible permutations.</param>
    /// <returns>A new array containing the elements of <paramref name="source" /> in randomized order.</returns>
    /// <remarks>
    /// This method never mutates the input array; it clones it and shuffles the clone in-place.
    /// Deterministic shuffling (via <paramref name="seed" />) is useful to assert order-invariance
    /// properties while keeping failing cases reproducible.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="source" /> is <c>null</c>.</exception>
    public static T[] Shuffle<T>(T[] source, int seed)
    {
        ArgumentNullException.ThrowIfNull(source);

        var rng = new DeterministicRandom(seed);
        var arr = (T[])source.Clone();

        for (var i = arr.Length - 1; i > 0; i--)
        {
            var j = rng.Next(i + 1);
            (arr[i], arr[j]) = (arr[j], arr[i]);
        }

        return arr;
    }

    private struct DeterministicRandom
    {
        private ulong _state;

        public DeterministicRandom(int seed)
        {
            _state = unchecked((uint)seed) + 0x9E3779B97F4A7C15UL;
        }

        public int Next(int exclusiveUpperBound)
        {
            if (exclusiveUpperBound <= 0)
                throw new ArgumentOutOfRangeException(nameof(exclusiveUpperBound), "Upper bound must be positive.");

            return (int)(NextUInt64() % (uint)exclusiveUpperBound);
        }

        public long NextInt64() => unchecked((long)NextUInt64());

        private ulong NextUInt64()
        {
            _state += 0x9E3779B97F4A7C15UL;
            var value = _state;
            value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9UL;
            value = (value ^ (value >> 27)) * 0x94D049BB133111EBUL;
            return value ^ (value >> 31);
        }
    }
}
