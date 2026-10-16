using Squirix.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Caps concurrent failover tests, so elections and fault timing stay within their bounds on small CI agents.</summary>
/// <remarks>A failover test runs a whole cluster for its duration and measures election and recovery time against it.</remarks>
[Immutable]
public sealed class FailoverLimit : IParallelLimit
{
    /// <inheritdoc />
    public int Limit => 2;
}
