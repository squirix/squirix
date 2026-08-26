using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>
/// Per-test two-node cluster driven by a dedicated fake clock shared by both nodes. Each test owns
/// an isolated cluster and clock, so parallel tests never observe each other's time advances.
/// </summary>
[Immutable]
public abstract class CrossNodeClockTestBase : EndToEndTestBase, IAsyncLifetime
{
    private HostedCluster? _cluster;
    private TwoNodeNamedCaches<object?>? _clusterCaches;

    /// <summary>Initializes a new instance of the <see cref="CrossNodeClockTestBase" /> class.</summary>
    protected CrossNodeClockTestBase()
    {
        Clock = new FakeTimeProvider();
    }

    /// <summary>Gets the fake clock driving both nodes of this test's cluster. Advance it for deterministic expiry.</summary>
    protected FakeTimeProvider Clock { get; }

    /// <summary>Gets the object-typed named caches for both nodes of this test's cluster.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the test cluster is not initialized.</exception>
    protected TwoNodeNamedCaches<object?> Cluster => _clusterCaches ?? throw new InvalidOperationException("Test cluster is not initialized.");

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        _cluster = await HostedCluster.StartTwoNodeAsync(new TwoNodeStartOptions { TimeProvider = Clock }, cancellationToken: DefaultCancellationToken);
        var clientA = await _cluster.ConnectClientAsync("nodeA", DefaultCancellationToken);
        var clientB = await _cluster.ConnectClientAsync("nodeB", DefaultCancellationToken);
        _clusterCaches = await TwoNodeNamedCaches<object?>.CreateAsync(_cluster, clientA, clientB, DefaultCancellationToken, false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster != null)
            await _cluster.DisposeAsync();

        GC.SuppressFinalize(this);
    }
}
