using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>Shared two-node cluster fixture for multi-node v0.1 public <see cref="ICache{T}" /> integration tests.</summary>
[Immutable]
public abstract class CrossNodeTestBase : EndToEndTestBase
{
    /// <summary>Gets the shared two-node cluster fixture injected once per test class.</summary>
    [ClassDataSource<TwoNodeFixture>(Shared = SharedType.PerClass)]
    public required TwoNodeFixture Fixture { get; init; }

    /// <summary>Gets the shared object-typed named caches for both nodes.</summary>
    protected TwoNodeNamedCaches<object?> Cluster => Fixture.NamedCaches;

    /// <summary>Creates typed named-cache facades backed by the shared cluster.</summary>
    /// <typeparam name="T">Cached value type.</typeparam>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>Named caches for both nodes.</returns>
    protected ValueTask<TwoNodeNamedCaches<T>> GetNamedCachesAsync<T>(CancellationToken cancellationToken) => Fixture.CreateNamedCachesAsync<T>(cancellationToken);
}
