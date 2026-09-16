using Squirix.Attributes;
using Squirix.Client;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Shared fixtures for single-node v0.1 public <see cref="ICache{T}" /> integration tests.</summary>
[Immutable]
[ParallelLimiter<ClusterStartupLimit>]
public abstract class TestBase : EndToEndTestBase
{
    /// <summary>Gets the shared single-node cluster fixture injected once per test class.</summary>
    [ClassDataSource<SingleNodeFixture>(Shared = SharedType.PerClass)]
    public required SingleNodeFixture Fixture { get; init; }

    /// <summary>Gets the shared SDK client connected to the class cluster.</summary>
    protected ISquirixClient Client => Fixture.Client;
}
