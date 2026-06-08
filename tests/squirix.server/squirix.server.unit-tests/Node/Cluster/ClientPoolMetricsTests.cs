using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
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
    private const string PoolDrainingInstrumentName = "squirix_peer_pool_draining";
    private const string PoolPeerCountInstrumentName = "squirix_peer_pool_peer_count";
    private const string PoolSizeInstrumentName = "squirix_peer_pool_size";

    /// <summary>
    /// Ensures Dispose emits squirix_client_pool_disposals_total counter events.
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
    /// Validates observable gauges for pool size, peer count, and draining flag reflect current state.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task GaugesReflectSizePeerCountAndDraining()
    {
        var size = new ConcurrentBag<int>();
        var peerCount = new ConcurrentBag<int>();
        var draining = new ConcurrentBag<int>();

        var measurements = new Dictionary<string, ConcurrentBag<int>>(StringComparer.Ordinal)
        {
            [PoolSizeInstrumentName] = size,
            [PoolPeerCountInstrumentName] = peerCount,
            [PoolDrainingInstrumentName] = draining,
        };

        using var listener = new MeterListener();
        listener.InstrumentPublished = static (instrument, listener) =>
        {
            if (instrument.Meter.Name != MeterName)
                return;

            if (IsClientPoolGauge(instrument.Name))
                listener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) =>
        {
            if (measurements.TryGetValue(instrument.Name, out var bag))
                bag.Add(measurement);
        });

        listener.Start();

        var peers = BuildPeers(4);
        var pool = new ClientPool(peers, static _ => new CallPolicy());
        await using (pool.ConfigureAwait(false))
        {
            listener.RecordObservableInstruments();

            Assert.Contains(4, size);
            Assert.Contains(4, peerCount);
            Assert.Contains(0, draining);

            pool.BeginDrain();
            listener.RecordObservableInstruments();

            Assert.Contains(1, draining);
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
        var size = new ConcurrentBag<int>();
        var peerCount = new ConcurrentBag<int>();

        var measurements = new Dictionary<string, ConcurrentBag<int>>(StringComparer.Ordinal)
        {
            [PoolSizeInstrumentName] = size,
            [PoolPeerCountInstrumentName] = peerCount,
        };

        using var listener = new MeterListener();
        listener.InstrumentPublished = static (instrument, listener) =>
        {
            if (instrument.Meter.Name != MeterName)
                return;

            if (IsClientPoolGauge(instrument.Name))
                listener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) =>
        {
            if (measurements.TryGetValue(instrument.Name, out var bag))
                bag.Add(measurement);
        });

        listener.Start();

        var peers = BuildPeers(2);
        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        var anchor = pool.ForNode("n0");

        for (var i = 0; i < 256; i++)
        {
            _ = pool.ForNode(i % 2 == 0 ? "n0" : "n1");
            listener.RecordObservableInstruments();
        }

        Assert.Same(anchor, pool.ForNode("n0"));
        Assert.Contains(2, size);
        Assert.Contains(2, peerCount);
        Assert.DoesNotContain(size, static value => value > 2);
    }

    private static Peer[] BuildPeers(int n)
    {
        var peers = new Peer[n];
        for (var i = 0; i < n; i++)
            peers[i] = new Peer { NodeId = $"n{i}", Url = $"https://localhost:{6500 + i}" };

        return peers;
    }

    private static bool IsClientPoolGauge(string instrumentName) => instrumentName is PoolSizeInstrumentName or PoolPeerCountInstrumentName or PoolDrainingInstrumentName;
}
