using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>
/// Per-test single-node cluster driven by a dedicated fake clock. xUnit creates a new instance per
/// test method, so each test owns an isolated node and clock, and parallel tests never observe each
/// other's time advances.
/// </summary>
[Immutable]
public abstract class ClockTestBase : EndToEndTestBase, IAsyncLifetime
{
    private HostedCluster? _cluster;
    private ISquirixClient? _client;

    /// <summary>Initializes a new instance of the <see cref="ClockTestBase" /> class.</summary>
    protected ClockTestBase()
    {
        Clock = new FakeTimeProvider();
    }

    /// <summary>Gets the fake clock driving this test's node. Advance it instead of sleeping for deterministic expiry.</summary>
    protected FakeTimeProvider Clock { get; }

    /// <summary>Gets the SDK client connected to this test's node.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the test cluster is not initialized.</exception>
    protected ISquirixClient Client => _client ?? throw new InvalidOperationException("Test cluster is not initialized.");

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        _cluster = await HostedCluster.StartSingleNodeAsync(timeProvider: Clock, cancellationToken: DefaultCancellationToken);
        _client = await _cluster.ConnectClientAsync(cancellationToken: DefaultCancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster != null)
            await _cluster.DisposeAsync();

        GC.SuppressFinalize(this);
    }
}
