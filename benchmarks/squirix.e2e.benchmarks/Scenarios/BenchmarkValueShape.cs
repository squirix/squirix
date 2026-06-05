namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>
/// Value shape used by a benchmark scenario.
/// </summary>
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
