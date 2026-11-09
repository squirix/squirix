using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>Shared two-node cluster fixture for multi-node v0.1 public <see cref="ICache{T}" /> integration tests.</summary>
[Immutable]
[ParallelLimiter<ClusterStartupLimit>]
public abstract class CrossNodeTestBase
{
    /// <summary>Gets the shared two-node cluster fixture injected once per test class.</summary>
    [ClassDataSource<TwoNodeFixture>(Shared = SharedType.PerClass)]
    public required TwoNodeFixture Fixture { get; init; }

    /// <summary>Gets the shared object-typed named caches for both nodes.</summary>
    protected TwoNodeNamedCaches<object?> Cluster => Fixture.NamedCaches;
}
