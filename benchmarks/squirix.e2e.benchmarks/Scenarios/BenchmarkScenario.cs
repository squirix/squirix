using System;
using System.Collections.Generic;

namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>
/// A stable benchmark scenario descriptor shown in BenchmarkDotNet output.
/// </summary>
/// <param name="Topology">The end-to-end topology.</param>
/// <param name="ValueShape">The cache value shape.</param>
/// <param name="DurabilityMode">The durability mode.</param>
public sealed record BenchmarkScenario(
    BenchmarkTopology Topology,
    BenchmarkValueShape ValueShape,
    E2EBenchmarkDurabilityMode DurabilityMode)
{
    /// <summary>
    /// Creates the default diagnostic scenario matrix.
    /// </summary>
    /// <returns>The default scenario matrix.</returns>
    public static IReadOnlyList<BenchmarkScenario> CreateDefaultMatrix()
    {
        if (string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_SMOKE"), "1", StringComparison.Ordinal))
            return [new BenchmarkScenario(BenchmarkTopology.SingleNode, BenchmarkValueShape.SmallString, E2EBenchmarkDurabilityMode.Default)];

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

        var scenarios = new List<BenchmarkScenario>(topologies.Length * shapes.Length);
        foreach (var topology in topologies)
        {
            foreach (var shape in shapes)
                scenarios.Add(new BenchmarkScenario(topology, shape, E2EBenchmarkDurabilityMode.Default));
        }

        return scenarios;
    }

    /// <inheritdoc />
    public override string ToString() => $"{Topology}-{ValueShape}-{DurabilityMode}";
}
