using System;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Percentage gate over stored performance evidence.</summary>
public static class PerformanceEvidenceGate
{
    /// <summary>Evidence schema identifier stored per phase.</summary>
    /// <remarks>v2 adds the OS family token to the machine fingerprint; no v1 evidence was ever stored.</remarks>
    public const string EvidenceSchema = "squirix.perf-evidence/v2";

    /// <summary>Checks the percentage gate, requiring a matching machine fingerprint.</summary>
    /// <param name="evidence">Stored performance evidence.</param>
    /// <param name="currentMachine">Machine fingerprint of the current host.</param>
    /// <returns>True when the measurement is within the allowed regression.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="evidence" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when fingerprints, names, or the benchmark-to-phase mapping is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when baseline, actual, allowed regression values, or the phase is invalid.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the machine fingerprint does not match.</exception>
    public static bool Check(PerformanceEvidence evidence, string currentMachine)
    {
        ArgumentNullException.ThrowIfNull(evidence);
        ArgumentException.ThrowIfNullOrEmpty(currentMachine);
        ArgumentException.ThrowIfNullOrEmpty(evidence.Machine);
        ArgumentException.ThrowIfNullOrEmpty(evidence.BenchmarkName);
        ArgumentException.ThrowIfNullOrEmpty(evidence.Phase);

        var expectedBenchmark = ReplicationBenchmarkCatalog.GetBenchmarkForPhase(evidence.Phase);
        if (!string.Equals(evidence.BenchmarkName, expectedBenchmark, StringComparison.Ordinal))
            throw new ArgumentException($"Evidence benchmark '{evidence.BenchmarkName}' does not match phase '{evidence.Phase}' benchmark '{expectedBenchmark}'.", nameof(evidence));

        if (!MachineFingerprint.Matches(evidence.Machine, currentMachine))
            throw new InvalidOperationException("Performance evidence machine fingerprint does not match the current host.");

        if (!double.IsFinite(evidence.Baseline) || evidence.Baseline <= 0)
            throw new ArgumentOutOfRangeException(nameof(evidence), evidence.Baseline, "Baseline must be a finite positive value.");

        if (!double.IsFinite(evidence.Actual) || evidence.Actual < 0)
            throw new ArgumentOutOfRangeException(nameof(evidence), evidence.Actual, "Actual must be a finite non-negative value.");

        if (!double.IsFinite(evidence.AllowedRegressionPercent) || evidence.AllowedRegressionPercent < 0)
            throw new ArgumentOutOfRangeException(nameof(evidence), evidence.AllowedRegressionPercent, "Allowed regression percent must be a finite non-negative value.");

        var ceiling = evidence.Baseline * (1.0 + (evidence.AllowedRegressionPercent / 100.0));
        return evidence.Actual <= ceiling;
    }
}
