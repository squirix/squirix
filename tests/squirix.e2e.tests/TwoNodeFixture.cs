using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>Shared two-node cluster and SDK clients for one public API test class.</summary>
[UsedImplicitly]
public sealed class TwoNodeFixture : NodeFixtureBase, IAsyncLifetime
{
    private ISquirixClient? _clientA;
    private ISquirixClient? _clientB;
    private HostedCluster? _cluster;
    private TwoNodeNamedCaches<object?>? _namedCaches;

    /// <summary>Gets the shared object-typed named caches for both nodes.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the fixture is not initialized.</exception>
    public TwoNodeNamedCaches<object?> NamedCaches => _namedCaches ?? ThrowFixtureNotInitialized();

    /// <summary>Creates typed named-cache facades backed by the shared cluster clients.</summary>
    /// <typeparam name="T">Cached value type.</typeparam>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Named caches for both nodes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the fixture is not initialized.</exception>
    public ValueTask<TwoNodeNamedCaches<T>> CreateNamedCachesAsync<T>(CancellationToken cancellationToken)
    {
        var initialized = _cluster != null && _clientA != null && _clientB != null;
        return initialized ? TwoNodeNamedCaches<T>.CreateAsync(_cluster!, _clientA!, _clientB!, cancellationToken, false)
            : throw new InvalidOperationException("Fixture is not initialized.");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster != null)
            await _cluster.DisposeAsync();
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        _cluster = await HostedCluster.StartTwoNodeAsync(nameof(TwoNodeFixture), cancellationToken: DefaultCancellationToken);
        _clientA = await _cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
        _clientB = await _cluster.ConnectClientAsync("nodeB", DefaultCancellationToken);
        _namedCaches = await TwoNodeNamedCaches<object?>.CreateAsync(_cluster, _clientA, _clientB, DefaultCancellationToken, false);
    }

    private static TwoNodeNamedCaches<object?> ThrowFixtureNotInitialized() => throw new InvalidOperationException("Fixture is not initialized.");
}
