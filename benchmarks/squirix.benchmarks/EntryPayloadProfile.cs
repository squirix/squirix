using System.Diagnostics.CodeAnalysis;

namespace Squirix.Benchmarks;

/// <summary>
/// Payload sizes used to compare serialization overhead across typical and near-limit entries.
/// </summary>
[SuppressMessage(
    "Maintainability",
    "CA1515:Consider making public types internal",
    Justification = "Benchmark parameter enum is consumed by the benchmark class and BenchmarkDotNet.")]
public enum EntryPayloadProfile
{
    /// <summary>A 256-byte string payload.</summary>
    Small256B,

    /// <summary>A 64 KiB string payload.</summary>
    Medium64KiB,

    /// <summary>A 1 MiB string payload.</summary>
    Large1MiB,

    /// <summary>A string payload at the discriminated entry size limit.</summary>
    NearLimitDiscriminated,
}
