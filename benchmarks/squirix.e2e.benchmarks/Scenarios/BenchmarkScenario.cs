using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>
/// A stable benchmark scenario descriptor shown in BenchmarkDotNet output.
/// </summary>
/// <param name="Topology">The end-to-end topology.</param>
/// <param name="ValueShape">The cache value shape.</param>
/// <param name="DurabilityMode">The durability mode.</param>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark scenario record is part of public benchmark parameterization.")]
public sealed record BenchmarkScenario(BenchmarkTopology Topology, BenchmarkValueShape ValueShape, E2EBenchmarkDurabilityMode DurabilityMode)
{
    /// <summary>
    /// Creates the default diagnostic scenario matrix.
    /// </summary>
    /// <returns>The default scenario matrix.</returns>
    public static IReadOnlyList<BenchmarkScenario> CreateDefaultMatrix()
    {
        if (string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_SMOKE"), "1", StringComparison.Ordinal))
            return CreateDurabilityComparisonMatrix();

        var durabilityModes = string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_DURABILITY"), "1", StringComparison.Ordinal)
            ? new[] { E2EBenchmarkDurabilityMode.Ephemeral, E2EBenchmarkDurabilityMode.Persistence } : new[] { E2EBenchmarkDurabilityMode.Ephemeral };

        var topologies = new[]
        {
            BenchmarkTopology.SingleNode,
            BenchmarkTopology.TwoNodeLocalOwner,
            BenchmarkTopology.TwoNodeRemoteOwner,
            BenchmarkTopology.TwoNodeUniformKeys,
            BenchmarkTopology.TwoNodeHotKeys,
        };

        var shapes = new[]
        {
            BenchmarkValueShape.PrimitiveLong,
            BenchmarkValueShape.SmallString,
            BenchmarkValueShape.SmallCustomRecord,
            BenchmarkValueShape.NestedCustomClass,
        };

        var scenarios = new List<BenchmarkScenario>(topologies.Length * shapes.Length * durabilityModes.Length);
        foreach (var topology in topologies)
        {
            foreach (var shape in shapes)
            {
                foreach (var durabilityMode in durabilityModes)
                    scenarios.Add(new BenchmarkScenario(topology, shape, durabilityMode));
            }
        }

        return scenarios;
    }

    /// <summary>
    /// Creates the focused single-node durability comparison matrix.
    /// </summary>
    /// <returns>The durability comparison scenario matrix.</returns>
    public static IReadOnlyList<BenchmarkScenario> CreateDurabilityComparisonMatrix() =>
    [
        new(BenchmarkTopology.SingleNode, BenchmarkValueShape.SmallString, E2EBenchmarkDurabilityMode.Ephemeral),
        new(BenchmarkTopology.SingleNode, BenchmarkValueShape.SmallString, E2EBenchmarkDurabilityMode.Persistence),
    ];

    /// <inheritdoc />
    public override string ToString() => $"{Topology}-{ValueShape}-{DurabilityMode}";
}
