using Squirix.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Caps concurrent failover tests, so elections and fault timing stay within their bounds on small CI agents.</summary>
/// <remarks>A failover test runs a whole cluster for its duration and measures election and recovery time against it.</remarks>
[Immutable]
public sealed class FailoverLimit : IParallelLimit
{
    /// <summary>
    /// The parallel group of the failover tests: they run in a batch of their own, never beside the other cluster-backed tests, as the CI runs
    /// them in a job of their own; within the batch <see cref="Limit" /> caps them.
    /// </summary>
    internal const string Group = "failover";

    /// <inheritdoc />
    public int Limit => 2;
}
