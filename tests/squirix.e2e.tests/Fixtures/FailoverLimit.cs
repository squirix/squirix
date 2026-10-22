using System;
using Squirix.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Caps concurrent failover tests, so elections and fault timing stay within their bounds on small CI agents.</summary>
/// <remarks>
/// A failover test runs a whole cluster for its duration and measures election and recovery time against it. Machines with fewer than
/// four cores run two at once, machines with four or more run at least three, and machines with many cores run one per four cores;
/// cluster startups stay serialized by their own limit either way.
/// </remarks>
[Immutable]
public sealed class FailoverLimit : IParallelLimit
{
    private const int MinLimit = 2;
    private const int FourCoreMinLimit = 3;
    private const int FourCoreProcessorCount = 4;
    private const int MaxLimit = 6;
    private const int ProcessorsPerFailoverTest = 4;

    /// <inheritdoc />
    public int Limit
    {
        get
        {
            var processorCount = Environment.ProcessorCount;
            var minLimit = processorCount >= FourCoreProcessorCount ? FourCoreMinLimit : MinLimit;
            return Math.Clamp(processorCount / ProcessorsPerFailoverTest, minLimit, MaxLimit);
        }
    }
}
