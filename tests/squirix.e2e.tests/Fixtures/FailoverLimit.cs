using System;
using Squirix.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Caps concurrent failover tests, so elections and fault timing stay within their bounds on small CI agents.</summary>
/// <remarks>
/// A failover test runs a whole cluster for its duration and measures election and recovery time against it. Machines with many cores
/// run more of them at once; cluster startups stay serialized by their own limit either way.
/// </remarks>
[Immutable]
public sealed class FailoverLimit : IParallelLimit
{
    private const int MinLimit = 2;
    private const int MaxLimit = 6;
    private const int ProcessorsPerFailoverTest = 4;

    /// <inheritdoc />
    public int Limit => Math.Clamp(Environment.ProcessorCount / ProcessorsPerFailoverTest, MinLimit, MaxLimit);
}
