using System;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Canonical registry of required replication benchmarks for release evidence.</summary>
/// <remarks>
/// All views (required benchmarks, phases, phase→benchmark and phase→schema maps) derive from the single
/// <c language="csharp">Entries</c> table below, so adding a benchmark cannot leave the views out of sync.
/// </remarks>
public static class ReplicationBenchmarkCatalog
{
    private static readonly (string Phase, string Benchmark, string Schema)[] Entries =
    [
        ("placement", "Squirix.Server.Benchmarks.ReplicaPlacementBenchmarks", "squirix.perf-evidence/v1"),
        ("follower-append", "Squirix.Server.Benchmarks.FollowerAppendBenchmarks", "squirix.perf-evidence/v1"),
        ("snapshot", "Squirix.Server.Benchmarks.ReplicaSnapshotBenchmarks", "squirix.perf-evidence/v1"),
        ("commit", "Squirix.E2EBenchmarks.Cache.ReplicaCommitBenchmarks", "squirix.perf-evidence/v1"),
        ("repair", "Squirix.Server.Benchmarks.ReplicaRepairBenchmarks", "squirix.perf-evidence/v1"),
        ("failover", "Squirix.E2EBenchmarks.Cache.FailoverBenchmarks", "squirix.perf-evidence/v1"),
        ("authority", "Squirix.E2EBenchmarks.Cache.LeaderAuthorityBenchmarks", "squirix.perf-evidence/v1"),
        ("sdk-operations", "Squirix.E2EBenchmarks.Cache.PublicSdkOperationsBenchmarks", "squirix.perf-evidence/v1"),
    ];

    private static readonly FrozenDictionary<string, string> PhaseBenchmarks = BuildPhaseBenchmarks();

    private static readonly FrozenDictionary<string, string> PhaseEvidenceSchemas = BuildPhaseEvidenceSchemas();

    /// <summary>Gets the required replication benchmark full type names.</summary>
    public static IReadOnlyList<string> RequiredBenchmarks { get; } = CollectBenchmarks();

    /// <summary>Gets the evidence phases, one per required benchmark.</summary>
    public static IReadOnlyList<string> Phases { get; } = CollectPhases();

    /// <summary>Gets the replica factors covered by RF-parameterized benchmark methods.</summary>
    /// <remarks>
    /// Coverage comes from the RF-parameterized methods of <c language="csharp">ReplicaPlacementBenchmarks</c>
    /// (<c language="csharp">GetReplicaGroupRfOne/Two/Three/Five</c>), not from multi-node harness size. RF=4 has
    /// no dedicated placement method: the ring walk is RF-agnostic and factors 1/2/3/5 span the quorum shapes
    /// (single copy, mirror, minimal quorum, maximum).
    /// </remarks>
    public static IReadOnlyList<int> CoveredReplicaFactors { get; } = [1, 2, 3, 5];

    /// <summary>Determines whether the specified replica factor is covered.</summary>
    /// <param name="replicaFactor">Replica factor.</param>
    /// <returns>True when the replica factor is covered.</returns>
    public static bool CoversReplicaFactor(int replicaFactor)
    {
        for (var i = 0; i < CoveredReplicaFactors.Count; i++)
        {
            if (CoveredReplicaFactors[i] == replicaFactor)
                return true;
        }

        return false;
    }

    /// <summary>Gets the benchmark full type name responsible for the specified phase.</summary>
    /// <param name="phase">Evidence phase name.</param>
    /// <returns>Benchmark full type name.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="phase" /> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="phase" /> is unknown.</exception>
    public static string GetBenchmarkForPhase(string phase)
    {
        ArgumentException.ThrowIfNullOrEmpty(phase);
        if (PhaseBenchmarks.TryGetValue(phase, out var benchmark))
            return benchmark;

        throw new ArgumentOutOfRangeException(nameof(phase), phase, "Unknown performance evidence phase.");
    }

    /// <summary>Gets the stored evidence schema identifier for the specified phase.</summary>
    /// <param name="phase">Evidence phase name.</param>
    /// <returns>Evidence schema identifier.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="phase" /> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="phase" /> is unknown.</exception>
    public static string GetEvidenceSchemaForPhase(string phase)
    {
        ArgumentException.ThrowIfNullOrEmpty(phase);
        if (PhaseEvidenceSchemas.TryGetValue(phase, out var schema))
            return schema;

        throw new ArgumentOutOfRangeException(nameof(phase), phase, "Unknown performance evidence phase.");
    }

    /// <summary>Determines whether the specified benchmark backs an evidence phase.</summary>
    /// <param name="benchmarkFullName">Benchmark full type name.</param>
    /// <returns>True when the benchmark is mapped to a phase.</returns>
    public static bool IsMappedToPhase(string benchmarkFullName)
    {
        ArgumentException.ThrowIfNullOrEmpty(benchmarkFullName);
        for (var i = 0; i < Entries.Length; i++)
        {
            if (string.Equals(Entries[i].Benchmark, benchmarkFullName, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>Determines whether every phase has the stored evidence schema.</summary>
    /// <returns>True when every phase maps to <see cref="PerformanceEvidenceGate.EvidenceSchema" />.</returns>
    public static bool EveryPhaseHasStoredEvidenceSchema()
    {
        for (var i = 0; i < Entries.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(Entries[i].Phase) || string.IsNullOrWhiteSpace(Entries[i].Benchmark))
                return false;

            if (!string.Equals(Entries[i].Schema, PerformanceEvidenceGate.EvidenceSchema, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static FrozenDictionary<string, string> BuildPhaseBenchmarks()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < Entries.Length; i++)
            map[Entries[i].Phase] = Entries[i].Benchmark;

        return map.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static FrozenDictionary<string, string> BuildPhaseEvidenceSchemas()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < Entries.Length; i++)
            map[Entries[i].Phase] = Entries[i].Schema;

        return map.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static string[] CollectBenchmarks()
    {
        var result = new string[Entries.Length];
        for (var i = 0; i < Entries.Length; i++)
            result[i] = Entries[i].Benchmark;

        return result;
    }

    private static string[] CollectPhases()
    {
        var result = new string[Entries.Length];
        for (var i = 0; i < Entries.Length; i++)
            result[i] = Entries[i].Phase;

        return result;
    }
}
