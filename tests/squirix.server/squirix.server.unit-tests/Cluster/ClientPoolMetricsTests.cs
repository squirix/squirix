using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Tests for ServerClientPool methods and metrics.</summary>
[Immutable]
public sealed class ClientPoolMetricsTests : DisposableServerUnitTestBase
{
    private const string PoolDisposalsTotalInstrumentName = "squirix_peer_pool_disposals_total";

    private readonly Meter _testMeter = new("test");

    /// <summary>Case-distinct node identities require distinct transport resources.</summary>
    [Test]
    public async Task CaseDistinctNodeIdsUseSeparatePools()
    {
        ServerPeer[] peers =
        [
            new() { NodeId = "node-a", Uri = new Uri("https://localhost:6500") },
            new() { NodeId = "NODE-A", Uri = new Uri("https://localhost:6501") },
        ];
        await using var pool = new ServerClientPool(peers, PolicyOnlyArgs(), new ServerClientPoolMetrics(_testMeter));

        _ = await Assert.That(pool.ForNode("NODE-A")).IsNotSameReferenceAs(pool.ForNode("node-a"));
        _ = await Assert.That(pool.PolicyFor("NODE-A")).IsNotSameReferenceAs(pool.PolicyFor("node-a"));
    }

    /// <summary>internode address rewrite rejects a non-absolute primary peer URI.</summary>
    [Test]
    public async Task ConstructorRejectsRelativePeerUri()
    {
        var peers = new ServerPeer[]
        {
            new() { NodeId = "n0", Uri = new Uri("relative-peer", UriKind.Relative) },
        };
        var args = new ServerClientPoolArgs
        {
            InterNodeMtlsEnabled = true,
            MtlsOptions = new MtlsOptions { InternalListenPort = 6101 },
            PolicyFactory = _ => new ServerCallPolicy(new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter))),
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            peers,
            args,
            _testMeter,
            static (peerList, poolArgs, meter) => _ = new ServerClientPool(peerList, poolArgs, new ServerClientPoolMetrics(meter)));
        _ = await Assert.That(ex.Message).IsEqualTo("Cluster peer URI is invalid.");
    }

    /// <summary>Ensures Dispose emits squirix_peer_pool_disposals_total counter events.</summary>
    [Test]
    public async Task DisposeIncrementsDisposalsTotal()
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        var peers = BuildPeers(2);
        var pool = new ServerClientPool(peers, PolicyOnlyArgs(), new ServerClientPoolMetrics(meter));

        await pool.DisposeAsync();

        _ = await Assert.That(sink.HasEvent(PoolDisposalsTotalInstrumentName)).IsTrue();
    }

    /// <summary>Repeated lookups for the same node must return the same gRPC client instance.</summary>
    [Test]
    public async Task ForNodeReusesSameClientAcrossManyLookups()
    {
        var peers = BuildPeers(1);
        await using var pool = new ServerClientPool(peers, PolicyOnlyArgs(), new ServerClientPoolMetrics(_testMeter));
        var first = pool.ForNode("n0");

        for (var i = 0; i < 256; i++)
            _ = await Assert.That(pool.ForNode("n0")).IsSameReferenceAs(first);
    }

    /// <summary>Ensures NodeIds is a deterministic snapshot of the pool membership.</summary>
    [Test]
    public async Task NodeIdsReturnsStableSortedSnapshot()
    {
        var peers = BuildPeers(3);
        await using var pool = new ServerClientPool(peers, PolicyOnlyArgs(), new ServerClientPoolMetrics(_testMeter));

        await SequenceAssert.Equal(["n0", "n1", "n2"], pool.NodeIds);
    }

    /// <summary>Many ForNode lookups must not grow the pooled channel count beyond the configured peer set.</summary>
    [Test]
    public async Task PoolSizeStableAcrossManyForNodeLookups()
    {
        var peers = BuildPeers(2);
        await using var pool = new ServerClientPool(peers, PolicyOnlyArgs(), new ServerClientPoolMetrics(_testMeter));

        var anchor = pool.ForNode("n0");

        for (var i = 0; i < 256; i++)
            _ = pool.ForNode(i % 2 == 0 ? "n0" : "n1");

        _ = await Assert.That(pool.ForNode("n0")).IsSameReferenceAs(anchor);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static ServerPeer[] BuildPeers(int n)
    {
        var peers = new ServerPeer[n];
        for (var i = 0; i < n; i++)
        {
            var nodeId = $"n{NodeInvariantIndexStrings.Format(i)}";
            peers[i] = new ServerPeer { NodeId = nodeId, Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", 6500 + i)) };
        }

        return peers;
    }

    private ServerClientPoolArgs PolicyOnlyArgs() => new() { PolicyFactory = _ => new ServerCallPolicy(new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter))) };
}
