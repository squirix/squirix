using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Support;
using Squirix.E2ETests.Support.Cluster;
using Squirix.E2ETests.Support.Cluster.Fixtures;
using Xunit;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>
/// Shared two-node cluster fixture for multi-node v0.1 public <see cref="ICache{T}" /> integration tests.
/// </summary>
public abstract class MultiNodeTestBase : EndToEndTestBase, IClassFixture<TwoNodeFixture>
{
    private readonly TwoNodeFixture _fixture;

    /// <summary>
    /// Initializes a new instance of the <see cref="MultiNodeTestBase" /> class.
    /// </summary>
    /// <param name="fixture">Shared two-node cluster fixture.</param>
    protected MultiNodeTestBase(TwoNodeFixture fixture) => _fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));

    /// <summary>Gets the shared object-typed named caches for both nodes.</summary>
    protected TwoNodeNamedCaches<object?> Cluster => _fixture.NamedCaches;

    /// <summary>Creates typed named-cache facades backed by the shared cluster.</summary>
    /// <typeparam name="T">Cached value type.</typeparam>
    /// <returns>Named caches for both nodes.</returns>
    protected ValueTask<TwoNodeNamedCaches<T>> GetNamedCachesAsync<T>() => _fixture.CreateNamedCachesAsync<T>(DefaultCancellationToken);
}
