using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Draws the random part of the election timeout of one replica group, deterministically from a seed.</summary>
/// <remarks>
/// The sequence is SplitMix64 over the options seed mixed with a stable hash of the group identifier, so two groups, and two nodes with
/// different seeds, draw different delays, while a pinned seed replays the same delays. The jitter only spreads elections apart to
/// break split votes; no safety property depends on it. Not thread safe: the election driver of the group is its only caller.
/// </remarks>
internal sealed class ElectionJitter
{
    private ulong _state;

    /// <summary>Initializes a new instance of the <see cref="ElectionJitter" /> class.</summary>
    /// <param name="seed">The seed of the options.</param>
    /// <param name="groupId">The replica group identifier mixed into the seed.</param>
    internal ElectionJitter(ulong seed, string groupId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        _state = seed ^ StableHash(groupId);
    }

    /// <summary>Draws the next delay, uniformly between zero and <paramref name="max" />, both included.</summary>
    /// <param name="max">The largest delay; zero or less draws zero.</param>
    /// <returns>The delay.</returns>
    internal TimeSpan Next(TimeSpan max) =>
        max <= TimeSpan.Zero ? TimeSpan.Zero : TimeSpan.FromTicks(long.CreateTruncating(NextUInt64() % (ulong.CreateTruncating(max.Ticks) + 1)));

    /// <summary>Computes the 64-bit FNV-1a hash of the UTF-16 code units of a string, stable across processes and runtimes.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The hash.</returns>
    private static ulong StableHash(string value)
    {
        var hash = 14695981039346656037UL;
        for (var i = 0; i < value.Length; i++)
        {
            hash ^= value[i];
            hash = unchecked(hash * 1099511628211UL);
        }

        return hash;
    }

    private ulong NextUInt64()
    {
        _state = unchecked(_state + 0x9E3779B97F4A7C15UL);
        var z = _state;
        z = unchecked((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL);
        z = unchecked((z ^ (z >> 27)) * 0x94D049BB133111EBUL);
        return z ^ (z >> 31);
    }
}
