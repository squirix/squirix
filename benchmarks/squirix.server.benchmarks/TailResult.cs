using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Benchmarks;

/// <summary>Measured journal write, flush and end-to-end latency of one <see cref="TailScenario" />.</summary>
[Immutable]
internal sealed class TailResult
{
    /// <summary>Initializes a new instance of the <see cref="TailResult" /> class.</summary>
    /// <param name="scenario">The measured scenario.</param>
    /// <param name="elapsed">The measured wall time.</param>
    /// <param name="endToEnd">End-to-end durable append latency samples.</param>
    /// <param name="flushes">FlushToDisk duration samples.</param>
    /// <param name="writes">Write duration samples.</param>
    /// <param name="probeMaxAge">The longest journal I/O call the stall probe saw in progress.</param>
    internal TailResult(TailScenario scenario, TimeSpan elapsed, LatencySamples endToEnd, LatencySamples flushes, LatencySamples writes, TimeSpan probeMaxAge)
    {
        Scenario = scenario;
        Elapsed = elapsed;
        EndToEnd = endToEnd;
        Flushes = flushes;
        Writes = writes;
        ProbeMaxAge = probeMaxAge;
    }

    /// <summary>Gets the end-to-end durable append latency samples.</summary>
    internal LatencySamples EndToEnd { get; }

    /// <summary>Gets the FlushToDisk duration samples.</summary>
    internal LatencySamples Flushes { get; }

    /// <summary>Gets the throughput during the measured window.</summary>
    internal double OpsPerSecond => EndToEnd.Count / Elapsed.TotalSeconds;

    /// <summary>Gets the longest journal I/O call the stall probe saw in progress.</summary>
    internal TimeSpan ProbeMaxAge { get; }

    /// <summary>Gets the measured scenario.</summary>
    internal TailScenario Scenario { get; }

    /// <summary>Gets the Write duration samples.</summary>
    internal LatencySamples Writes { get; }

    private TimeSpan Elapsed { get; }

    /// <summary>Gets the leading table cells identifying the scenario.</summary>
    /// <returns>The cells text, with a trailing pipe.</returns>
    internal string Key() => $"| {(Scenario.NoiseWriters == 0 ? "quiet" : $"{Scenario.NoiseWriters} noisy writers")} | {(Scenario.GroupCommit ? "on" : "off")} | {PayloadLabel()} | {Scenario.Writers} |";

    private string PayloadLabel() => Scenario.PayloadBytes >= 1024 ? $"{Scenario.PayloadBytes / 1024} KB" : $"{Scenario.PayloadBytes} B";
}
