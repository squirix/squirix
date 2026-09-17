using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>
/// Per-test single-node cluster driven by a dedicated fake clock. TUnit creates a new instance per
/// test method, so each test owns an isolated node and clock, and parallel tests never observe each
/// other's time advances.
/// </summary>
[Immutable]
[ParallelLimiter<ClusterStartupLimit>]
public abstract class ClockTestBase : EndToEndTestBase
{
    private HostedCluster? _cluster;

    /// <summary>Initializes a new instance of the <see cref="ClockTestBase" /> class.</summary>
    protected ClockTestBase()
    {
        Clock = new FakeTimeProvider();
    }

    /// <summary>Gets the SDK client connected to this test's node.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the test cluster is not initialized.</exception>
    protected ISquirixClient Client
    {
        get => E2EThrowHelper.Required(field, "Test cluster is not initialized.");
        private set;
    }

    /// <summary>Gets the fake clock driving this test's node. Advance it instead of sleeping for deterministic expiry.</summary>
    protected FakeTimeProvider Clock { get; }

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
        _cluster = await HostedCluster.StartSingleNodeAsync(timeProvider: Clock, cancellationToken: cancellationToken);
        Client = await _cluster.ConnectClientAsync(cancellationToken: cancellationToken);
    }
}
