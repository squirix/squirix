using System.Diagnostics.CodeAnalysis;

namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>
/// Value shape used by a benchmark scenario.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark parameter enum is part of public benchmark parameterization.")]
public enum BenchmarkValueShape
{
    /// <summary>
    /// A small primitive long value.
    /// </summary>
    PrimitiveLong,

    /// <summary>
    /// A small string value.
    /// </summary>
    SmallString,

    /// <summary>
    /// A compact immutable custom record.
    /// </summary>
    SmallCustomRecord,

    /// <summary>
    /// A mutable nested custom class.
    /// </summary>
    NestedCustomClass,
}
