using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Benchmarks;

/// <summary>Exact latency samples in microseconds with percentile and threshold-exceedance queries; every sample is kept so tails are not approximated.</summary>
[ThreadSafe]
internal sealed class LatencySamples
{
    private readonly Lock _gate = new();
    private readonly List<long> _micros = [];

    /// <summary>Gets the number of samples.</summary>
    internal int Count
    {
        get
        {
            lock (_gate)
                return _micros.Count;
        }
    }

    /// <summary>Adds one sample.</summary>
    /// <param name="elapsed">The measured duration.</param>
    internal void Add(TimeSpan elapsed)
    {
        var micros = Convert.ToInt64(elapsed.TotalMicroseconds);
        lock (_gate)
            _micros.Add(micros);
    }

    /// <summary>Copies every sample of <paramref name="other" /> into this instance.</summary>
    /// <param name="other">The samples to merge.</param>
    internal void AddRange(LatencySamples other)
    {
        ArgumentNullException.ThrowIfNull(other);
        long[] copy;
        lock (other._gate)
            copy = [.. other._micros];

        lock (_gate)
            _micros.AddRange(copy);
    }

    /// <summary>Counts the samples strictly above <paramref name="threshold" />.</summary>
    /// <param name="threshold">The threshold.</param>
    /// <returns>The number of samples above it.</returns>
    internal int CountAbove(TimeSpan threshold)
    {
        var limit = Convert.ToInt64(threshold.TotalMicroseconds);
        var count = 0;
        lock (_gate)
        {
            foreach (var micros in CollectionsMarshal.AsSpan(_micros))
            {
                if (micros > limit)
                    count++;
            }
        }

        return count;
    }

    /// <summary>Gets the largest sample in milliseconds.</summary>
    /// <returns>The maximum, or zero when there are no samples.</returns>
    internal double MaxMs() => Percentile(1.0);

    /// <summary>Gets a percentile in milliseconds (nearest rank).</summary>
    /// <param name="fraction">The fraction in [0, 1], for example 0.99.</param>
    /// <returns>The percentile, or zero when there are no samples.</returns>
    internal double Percentile(double fraction)
    {
        long[] sorted;
        lock (_gate)
            sorted = [.. _micros];

        if (sorted.Length == 0)
            return 0;

        Array.Sort(sorted);
        var rank = Convert.ToInt32(Math.Ceiling(fraction * sorted.Length));
        return sorted[Math.Clamp(rank - 1, 0, sorted.Length - 1)] / 1000.0;
    }
}
