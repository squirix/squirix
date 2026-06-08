using System;
using System.Threading.Tasks;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Node.Cluster.Reliability;
using Squirix.Server.Node.Cluster.Transport;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Cluster;

/// <summary>
/// Tests for ClientPool methods and metrics.
/// </summary>
public sealed class ClientPoolMetricsTests : ServerUnitTestBase
{
    private const string MeterName = "Squirix";
    private const string PoolDisposalsTotalInstrumentName = "squirix_peer_pool_disposals_total";

    /// <summary>
    /// Ensures Dispose emits squirix_peer_pool_disposals_total counter events.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task DisposeIncrementsDisposalsTotal()
    {
        using var sink = new MeasurementSink(MeterName);
        var peers = BuildPeers(2);
        var pool = new ClientPool(peers, static _ => new CallPolicy());

        await pool.DisposeAsync();

        Assert.True(sink.HasEvent(PoolDisposalsTotalInstrumentName));
    }

    /// <summary>
    /// Pool size matches configured peers and draining toggles after BeginDrain.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ActiveClientCountReflectsPeerSetAndDrainingState()
    {
        var peers = BuildPeers(4);
        var pool = new ClientPool(peers, static _ => new CallPolicy());
        await using (pool.ConfigureAwait(false))
        {
            Assert.Equal(4, pool.ActiveClientCount);
            Assert.Equal(4, pool.NodeIds.Count);
            Assert.False(pool.IsDraining);

            pool.BeginDrain();

            Assert.True(pool.IsDraining);
            Assert.Equal(4, pool.ActiveClientCount);
        }
    }

    /// <summary>
    /// Ensures NodeIds is a deterministic snapshot of the pool membership.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task NodeIdsReturnsStableSortedSnapshot()
    {
        var peers = BuildPeers(3);
        var pool = new ClientPool(peers, static _ => new CallPolicy());
        await using var poolHandle = pool;

        Assert.Equal(["n0", "n1", "n2"], pool.NodeIds);
    }

    /// <summary>
    /// Ensures WarmUpAsync fails fast when configured peer endpoints are unreachable.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task WarmUpThrowsWhenEndpointsAreUnreachable()
    {
        var peers = BuildPeers(1);
        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => pool.WarmUpAsync(DefaultCancellationToken).AsTask());
        Assert.Contains("Failed to connect to endpoint", exception.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Repeated lookups for the same node must return the same gRPC client instance.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ForNodeReusesSameClientAcrossManyLookups()
    {
        var peers = BuildPeers(1);
        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        var first = pool.ForNode("n0");

        for (var i = 0; i < 256; i++)
            Assert.Same(first, pool.ForNode("n0"));
    }

    /// <summary>
    /// Many ForNode lookups must not grow the pooled channel count beyond the configured peer set.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task PoolSizeRemainsStableAfterManyForNodeLookups()
    {
        var peers = BuildPeers(2);
        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        Assert.Equal(2, pool.ActiveClientCount);

        var anchor = pool.ForNode("n0");

        for (var i = 0; i < 256; i++)
            _ = pool.ForNode(i % 2 == 0 ? "n0" : "n1");

        Assert.Same(anchor, pool.ForNode("n0"));
        Assert.Equal(2, pool.ActiveClientCount);
    }

    private static Peer[] BuildPeers(int n)
    {
        var peers = new Peer[n];
        for (var i = 0; i < n; i++)
            peers[i] = new Peer { NodeId = $"n{i}", Url = $"https://localhost:{6500 + i}" };

        return peers;
    }
}
