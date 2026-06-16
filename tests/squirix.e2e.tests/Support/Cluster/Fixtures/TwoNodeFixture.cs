using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.E2ETests.Support.Cluster.Fixtures;

/// <summary>
/// Shared two-node cluster and SDK clients for one public API test class.
/// </summary>
[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Instantiated by xUnit via IClassFixture<T>.")]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Test fixture surface must be public for xUnit class fixtures.")]
public sealed class TwoNodeFixture : NodeFixtureBase, IAsyncLifetime
{
    private HostedCluster? _cluster;
    private ISquirixClient? _clientA;
    private ISquirixClient? _clientB;

    /// <summary>
    /// Gets the shared object-typed named caches for both nodes.
    /// </summary>
    public TwoNodeNamedCaches<object?> NamedCaches { get; private set; } = null!;

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        _cluster = await HostedCluster.StartTwoNodeAsync(nameof(TwoNodeFixture), cancellationToken: DefaultCancellationToken);
        _clientA = await _cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
        _clientB = await _cluster.ConnectClientAsync("nodeB", DefaultCancellationToken);
        NamedCaches = await TwoNodeNamedCaches<object?>.CreateAsync(_cluster, _clientA, _clientB, DefaultCancellationToken, false);
    }

    /// <summary>
    /// Creates typed named-cache facades backed by the shared cluster clients.
    /// </summary>
    /// <typeparam name="T">Cached value type.</typeparam>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Named caches for both nodes.</returns>
    public ValueTask<TwoNodeNamedCaches<T>> CreateNamedCachesAsync<T>(CancellationToken cancellationToken) =>
        TwoNodeNamedCaches<T>.CreateAsync(_cluster!, _clientA!, _clientB!, cancellationToken, false);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster is not null)
            await _cluster.DisposeAsync();
    }
}
