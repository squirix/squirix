using System;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Tests for ServerClientPool methods and metrics.</summary>
public sealed class ClientPoolMetricsTests : UnitTestBase
{
    private const string MeterName = "Squirix";
    private const string PoolDisposalsTotalInstrumentName = "squirix_peer_pool_disposals_total";

    /// <summary>Ensures Dispose emits squirix_peer_pool_disposals_total counter events.</summary>
    [Fact]
    public async Task DisposeIncrementsDisposalsTotal()
    {
        using var sink = new MeasurementSink(MeterName);
        var peers = BuildPeers(2);
        var pool = new ServerClientPool(peers, PolicyOnlyArgs());

        await pool.DisposeAsync();

        Assert.True(sink.HasEvent(PoolDisposalsTotalInstrumentName));
    }

    /// <summary>Repeated lookups for the same node must return the same gRPC client instance.</summary>
    [Fact]
    public async Task ForNodeReusesSameClientAcrossManyLookups()
    {
        var peers = BuildPeers(1);
        await using var pool = new ServerClientPool(peers, PolicyOnlyArgs());
        var first = pool.ForNode("n0");

        for (var i = 0; i < 256; i++)
            Assert.Same(first, pool.ForNode("n0"));
    }

    /// <summary>Ensures NodeIds is a deterministic snapshot of the pool membership.</summary>
    [Fact]
    public async Task NodeIdsReturnsStableSortedSnapshot()
    {
        var peers = BuildPeers(3);
        var pool = new ServerClientPool(peers, PolicyOnlyArgs());
        await using var poolHandle = pool;

        Assert.Equal(["n0", "n1", "n2"], pool.NodeIds);
    }

    /// <summary>Many ForNode lookups must not grow the pooled channel count beyond the configured peer set.</summary>
    [Fact]
    public async Task PoolSizeRemainsStableAfterManyForNodeLookups()
    {
        var peers = BuildPeers(2);
        await using var pool = new ServerClientPool(peers, PolicyOnlyArgs());

        var anchor = pool.ForNode("n0");

        for (var i = 0; i < 256; i++)
            _ = pool.ForNode(i % 2 is 0 ? "n0" : "n1");

        Assert.Same(anchor, pool.ForNode("n0"));
    }

    private static ServerClientPoolArgs PolicyOnlyArgs() => new() { PolicyFactory = static _ => new ServerCallPolicy() };

    private static ServerPeer[] BuildPeers(int n)
    {
        var peers = new ServerPeer[n];
        for (var i = 0; i < n; i++)
            peers[i] = new ServerPeer { NodeId = $"n{i.ToString(CultureInfo.InvariantCulture)}", Uri = new Uri($"https://localhost:{(6500 + i).ToString(CultureInfo.InvariantCulture)}") };

        return peers;
    }
}
