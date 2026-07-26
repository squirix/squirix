using System;
using System.Collections.Generic;

namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>A stable benchmark scenario descriptor shown in BenchmarkDotNet output.</summary>
/// <param name="Topology">The end-to-end topology.</param>
/// <param name="ValueShape">The cache value shape.</param>
/// <param name="DurabilityMode">The durability mode.</param>
public sealed record BenchmarkScenario(BenchmarkTopology Topology, BenchmarkValueShape ValueShape, E2EBenchmarkDurabilityMode DurabilityMode)
{
    private static readonly BenchmarkValueShape[] DefaultShapes =
    [
        BenchmarkValueShape.PrimitiveLong,
        BenchmarkValueShape.SmallString,
        BenchmarkValueShape.SmallCustomRecord,
        BenchmarkValueShape.NestedCustomClass,
    ];

    private static readonly BenchmarkTopology[] DefaultTopologies =
    [
        BenchmarkTopology.SingleNode,
        BenchmarkTopology.TwoNodeLocalOwner,
        BenchmarkTopology.TwoNodeRemoteOwner,
        BenchmarkTopology.TwoNodeUniformKeys,
        BenchmarkTopology.TwoNodeHotKeys,
    ];

    private static readonly E2EBenchmarkDurabilityMode[] EphemeralAndPersistent =
    [
        E2EBenchmarkDurabilityMode.Ephemeral,
        E2EBenchmarkDurabilityMode.Persistence,
    ];

    private static readonly E2EBenchmarkDurabilityMode[] EphemeralOnly = [E2EBenchmarkDurabilityMode.Ephemeral];

    /// <summary>Creates the default diagnostic scenario matrix.</summary>
    /// <returns>The default scenario matrix.</returns>
    public static IReadOnlyList<BenchmarkScenario> CreateDefaultMatrix()
    {
        if (string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_SMOKE"), "1", StringComparison.Ordinal))
            return CreateDurabilityComparisonMatrix();

        var durabilityModes = string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_E2E_BENCHMARK_DURABILITY"), "1", StringComparison.Ordinal) ? EphemeralAndPersistent
            : EphemeralOnly;

        var scenarios = new List<BenchmarkScenario>(DefaultTopologies.Length * DefaultShapes.Length * durabilityModes.Length);
        foreach (var topology in DefaultTopologies)
            foreach (var shape in DefaultShapes)
                foreach (var durabilityMode in durabilityModes)
                    scenarios.Add(new BenchmarkScenario(topology, shape, durabilityMode));

        return scenarios;
    }

    /// <summary>Creates the focused single-node durability comparison matrix.</summary>
    /// <returns>The durability comparison scenario matrix.</returns>
    public static IReadOnlyList<BenchmarkScenario> CreateDurabilityComparisonMatrix() =>
    [
        new(BenchmarkTopology.SingleNode, BenchmarkValueShape.SmallString, E2EBenchmarkDurabilityMode.Ephemeral),
        new(BenchmarkTopology.SingleNode, BenchmarkValueShape.SmallString, E2EBenchmarkDurabilityMode.Persistence),
    ];

    /// <inheritdoc />
    public override string ToString() => $"{Topology}-{ValueShape}-{DurabilityMode}";
}
