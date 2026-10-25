using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Squirix.Server.Attributes;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>What a <see cref="StateTransferWriteLoad" /> wrote.</summary>
/// <param name="SortedMicroseconds">The latency of every successful write in microseconds, ascending.</param>
/// <param name="Errors">The count of refused or failed writes by kind.</param>
/// <param name="Elapsed">The time from start to end.</param>
/// <param name="Stalls">The writes that took longer than a second, as the offset of their start from the start of the load and their duration.</param>
[Immutable]
internal sealed record StateTransferWriteResult(int[] SortedMicroseconds, IReadOnlyDictionary<string, int> Errors, TimeSpan Elapsed, IReadOnlyList<(TimeSpan At, TimeSpan Took)> Stalls)
{
    private const double MicrosecondsPerMillisecond = 1000.0;

    /// <summary>Gets the number of successful writes.</summary>
    internal int Count => SortedMicroseconds.Length;

    /// <summary>Formats the result as one line.</summary>
    /// <returns>Writes, rate, latency percentiles in milliseconds and the refusals.</returns>
    internal string Describe()
    {
        var errors = new StringBuilder();
        foreach (var pair in Errors)
        {
            if (errors.Length > 0)
                _ = errors.Append(',');

            _ = errors.Append(CultureInfo.InvariantCulture, $"{pair.Key}={pair.Value}");
        }

        var stalls = new StringBuilder();
        foreach (var (at, took) in Stalls)
            _ = stalls.Append(CultureInfo.InvariantCulture, $" [at {at.TotalSeconds:F1}s for {took.TotalSeconds:F1}s]");

        var rate = Elapsed.TotalSeconds > 0 ? Count / Elapsed.TotalSeconds : 0;
        return string.Create(
            CultureInfo.InvariantCulture,
            $"writes={Count} rate={rate:F0}/s p50={Percentile(0.50):F2}ms p99={Percentile(0.99):F2}ms p999={Percentile(0.999):F2}ms max={Percentile(1.0):F2}ms errors={(errors.Length == 0 ? "none" : errors.ToString())} stallsOver1s={Stalls.Count}{stalls}");
    }

    /// <summary>Gets a latency percentile.</summary>
    /// <param name="fraction">The fraction in (0, 1].</param>
    /// <returns>The latency in milliseconds; zero when nothing was written.</returns>
    internal double Percentile(double fraction) =>
        SortedMicroseconds.Length == 0 ? 0 : SortedMicroseconds[Math.Max(0, int.CreateChecked(Math.Ceiling(fraction * SortedMicroseconds.Length)) - 1)] / MicrosecondsPerMillisecond;
}
