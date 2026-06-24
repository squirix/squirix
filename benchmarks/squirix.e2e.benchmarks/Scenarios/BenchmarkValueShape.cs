namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>Value shape used by a benchmark scenario.</summary>
public enum BenchmarkValueShape
{
    /// <summary>A small primitive long value.</summary>
    PrimitiveLong = 0,

    /// <summary>A small string value.</summary>
    SmallString = 1,

    /// <summary>A compact immutable custom record.</summary>
    SmallCustomRecord = 2,

    /// <summary>A mutable nested custom class.</summary>
    NestedCustomClass = 3,
}
