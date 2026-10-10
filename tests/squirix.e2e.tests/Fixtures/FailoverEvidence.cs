using System;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Computes percentiles of recovery times and writes them, with the machine and the election timing, as a timing evidence file.</summary>
internal static class FailoverEvidence
{
    /// <summary>The name of the environment variable that names the directory evidence files are written to.</summary>
    internal const string DirectoryVariable = "SQUIRIX_EVIDENCE_DIR";

    /// <summary>The name of the environment variable that holds the fingerprint of the controlled machine the timing limit applies to.</summary>
    internal const string ControlledMachineVariable = "SQUIRIX_CONTROLLED_MACHINE";

    /// <summary>The schema identifier of the evidence file.</summary>
    internal const string Schema = "squirix.failover-timing/v1";

    /// <summary>The 95th percentile recovery time a leader stop must keep on the controlled machine.</summary>
    internal static readonly TimeSpan P95Limit = TimeSpan.FromSeconds(5);

    /// <summary>Gets the directory evidence files are written to: the variable when set, otherwise a folder next to the test binaries.</summary>
    /// <returns>The directory path.</returns>
    internal static string Directory()
    {
        var configured = Environment.GetEnvironmentVariable(DirectoryVariable);
        return string.IsNullOrWhiteSpace(configured) ? Path.Join(AppContext.BaseDirectory, "evidence") : configured;
    }

    /// <summary>Decides whether the timing limit applies on this host: only when it is the controlled machine, by its fingerprint.</summary>
    /// <returns>The machine fingerprint of this host, whether the limit applies, and the reason in words.</returns>
    internal static (string Machine, bool Enforced, string Reason) Gate()
    {
        var machine = MachineFingerprint.Compute();
        var controlled = Environment.GetEnvironmentVariable(ControlledMachineVariable);
        if (string.IsNullOrWhiteSpace(controlled))
            return (machine, false, $"informational: {ControlledMachineVariable} is not set");

        var matches = MachineFingerprint.Matches(controlled, machine);
        return (machine, matches, matches ? "enforced: the machine fingerprint matches the controlled machine" : $"informational: the machine fingerprint differs from the controlled machine '{controlled}'");
    }

    /// <summary>Checks the 95th percentile against the limit with the performance evidence gate of the testkit.</summary>
    /// <param name="machine">The fingerprint of this host, which must be the controlled machine.</param>
    /// <param name="p95Ms">The 95th percentile in milliseconds.</param>
    /// <returns><see langword="true" /> when the percentile is within the limit.</returns>
    internal static bool WithinLimit(string machine, double p95Ms) =>
        PerformanceEvidenceGate.Check(new PerformanceEvidence("Squirix.E2EBenchmarks.Cache.FailoverBenchmarks", "failover", machine, P95Limit.TotalMilliseconds, p95Ms, 0.0), machine);

    /// <summary>Gets a percentile of sorted values by the nearest rank method.</summary>
    /// <param name="sorted">The values, in ascending order; at least one.</param>
    /// <param name="percentile">The percentile, above zero and at most one hundred.</param>
    /// <returns>The value at that rank.</returns>
    internal static double Percentile(double[] sorted, double percentile)
    {
        ArgumentNullException.ThrowIfNull(sorted);
        ArgumentOutOfRangeException.ThrowIfZero(sorted.Length);
        var rank = Convert.ToInt32(Math.Ceiling(percentile / 100.0 * sorted.Length));
        return sorted[Math.Clamp(rank, 1, sorted.Length) - 1];
    }

    /// <summary>Builds the evidence of a series: sorts the recovery times, takes the median and the 95th percentile.</summary>
    /// <param name="test">The test that recorded the series.</param>
    /// <param name="timing">The election timing of the cluster.</param>
    /// <param name="samples">The samples recorded so far.</param>
    /// <param name="completed">Whether the series ran to the end.</param>
    /// <returns>The evidence.</returns>
    internal static FailoverTimingEvidence Build(string test, TestElectionTiming timing, FailoverSample[] samples, bool completed)
    {
        var times = new double[samples.Length];
        for (var i = 0; i < times.Length; i++)
            times[i] = samples[i].RecoveryMs;

        Array.Sort(times);
        var (machine, _, reason) = Gate();
        return new FailoverTimingEvidence(
            Schema,
            test,
            machine,
            RuntimeInformation.OSDescription,
            Environment.ProcessorCount,
            RuntimeInformation.FrameworkDescription,
            new ElectionTimingEvidence(
                timing.ElectionTimeout.TotalMilliseconds,
                timing.HeartbeatInterval.TotalMilliseconds,
                timing.MaxJitter.TotalMilliseconds,
                timing.VoteRpcTimeout.TotalMilliseconds,
                timing.JitterSeed ?? 0UL),
            reason,
            completed,
            P95Limit.TotalMilliseconds,
            times.Length == 0 ? 0.0 : Percentile(times, 50),
            times.Length == 0 ? 0.0 : Percentile(times, 95),
            samples);
    }

    /// <summary>Writes the evidence as indented JSON into the evidence directory.</summary>
    /// <param name="evidence">The evidence.</param>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The full path of the file.</returns>
    internal static async Task<string> WriteAsync(FailoverTimingEvidence evidence, string fileName, CancellationToken cancellationToken)
    {
        var directory = Directory();
        _ = System.IO.Directory.CreateDirectory(directory);
        var path = Path.Join(directory, fileName);
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(evidence, FailoverEvidenceJsonContext.Default.FailoverTimingEvidence), cancellationToken);
        return path;
    }

    /// <summary>Renders a sample for the test output.</summary>
    /// <param name="sample">The sample.</param>
    /// <returns>One line.</returns>
    internal static string Describe(FailoverSample sample)
    {
        ArgumentNullException.ThrowIfNull(sample);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"#{sample.Iteration,2} {sample.StoppedLeader} -> {sample.NewLeader} term {sample.NewTerm}: recovery {sample.RecoveryMs:F0} ms, stop {sample.StopMs:F0} ms, leader lost {Format(sample.LeaderLostMs)}, term raised {Format(sample.TermRaisedMs)}, new leader {Format(sample.NewLeaderMs)}, converged {Format(sample.ConvergedMs)}");
    }

    private static string Format(double? milliseconds) => milliseconds is { } value ? string.Create(CultureInfo.InvariantCulture, $"{value:F0} ms") : "not seen";
}
