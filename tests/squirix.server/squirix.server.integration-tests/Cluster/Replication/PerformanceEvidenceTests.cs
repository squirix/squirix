using System;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Benchmarks;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Percentage gates over stored performance evidence with machine fingerprints.</summary>
public sealed class PerformanceEvidenceTests : NodeIntegrationTestBase
{
    /// <summary>The percentage gate requires a matching machine fingerprint before comparing values.</summary>
    /// <remarks>
    /// #239 mandates the name "PercentageGateRequiresMatchingMachineFingerprint"; it is shortened here because
    /// SQR0005 limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Fact]
    public void PercentageGateRequiresMachineFingerprint()
    {
        var machine = MachineFingerprint.Compute();
        Assert.False(string.IsNullOrWhiteSpace(machine));
        Assert.True(MachineFingerprint.Matches(machine, MachineFingerprint.Compute()));

        var within = new PerformanceEvidence("Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks", "placement", machine, 100.0, 105.0, 10.0);
        Assert.True(PerformanceEvidenceGate.Check(within, machine));

        var regressed = new PerformanceEvidence("Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks", "placement", machine, 100.0, 120.0, 10.0);
        Assert.False(PerformanceEvidenceGate.Check(regressed, machine));

        var foreign = new PerformanceEvidence("Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks", "placement", machine + "|foreign", 100.0, 100.0, 10.0);
        var exception = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            foreign,
            machine,
            static (evidence, current) => _ = PerformanceEvidenceGate.Check(evidence, current));
        Assert.Contains("fingerprint", exception.Message, StringComparison.OrdinalIgnoreCase);

        // Misconfigured evidence fails loud instead of masquerading as a regression verdict.
        var zeroBaseline = new PerformanceEvidence("Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks", "placement", machine, 0.0, 100.0, 10.0);
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            zeroBaseline,
            machine,
            static (evidence, current) => _ = PerformanceEvidenceGate.Check(evidence, current));

        var negativeActual = new PerformanceEvidence("Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks", "placement", machine, 100.0, -1.0, 10.0);
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            negativeActual,
            machine,
            static (evidence, current) => _ = PerformanceEvidenceGate.Check(evidence, current));

        // Non-finite values fail loud instead of skewing the ceiling comparison.
        var nanBaseline = new PerformanceEvidence("Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks", "placement", machine, double.NaN, 100.0, 10.0);
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            nanBaseline,
            machine,
            static (evidence, current) => _ = PerformanceEvidenceGate.Check(evidence, current));
    }
}
