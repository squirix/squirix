using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode;

/// <summary>
/// Per-test two-node cluster driven by a dedicated fake clock shared by both nodes. Each test owns
/// an isolated cluster and clock, so parallel tests never observe each other's time advances.
/// </summary>
[Immutable]
[ParallelLimiter<ClusterStartupLimit>]
public abstract class CrossNodeClockTestBase : EndToEndTestBase
{
    private HostedCluster? _cluster;

    /// <summary>Initializes a new instance of the <see cref="CrossNodeClockTestBase" /> class.</summary>
    protected CrossNodeClockTestBase()
    {
        Clock = new FakeTimeProvider();
    }

    /// <summary>Gets the fake clock driving both nodes of this test's cluster. Advance it for deterministic expiry.</summary>
    protected FakeTimeProvider Clock { get; }

    /// <summary>Gets the object-typed named caches for both nodes of this test's cluster.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the test cluster is not initialized.</exception>
    protected TwoNodeNamedCaches<object?> Cluster
    {
        get => E2EThrowHelper.Required(field, "Test cluster is not initialized.");
        private set;
    }

    /// <summary>Disposes this test's cluster.</summary>
    [After(HookType.Test)]
    public async Task DisposeAsync()
    {
        if (_cluster != null)
            await _cluster.DisposeAsync();
    }

    /// <summary>Starts this test's isolated cluster.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Before(HookType.Test)]
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        _cluster = await HostedCluster.StartTwoNodeAsync(new MultiNodeStartOptions { TimeProvider = Clock }, cancellationToken: cancellationToken);
        var clientA = await _cluster.ConnectClientAsync("nodeA", cancellationToken);
        var clientB = await _cluster.ConnectClientAsync("nodeB", cancellationToken);
        Cluster = await TwoNodeNamedCaches<object?>.CreateAsync(_cluster, clientA, clientB, cancellationToken, false);
    }
}
