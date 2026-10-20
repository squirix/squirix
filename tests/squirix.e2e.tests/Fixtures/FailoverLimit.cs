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
    private const int SmallMachineLimit = 2;
    private const int LargeMachineLimit = 4;
    private const int LargeMachineProcessorCount = 16;

    /// <inheritdoc />
    public int Limit => Environment.ProcessorCount >= LargeMachineProcessorCount ? LargeMachineLimit : SmallMachineLimit;
}
