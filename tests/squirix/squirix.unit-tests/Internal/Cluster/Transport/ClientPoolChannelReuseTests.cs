using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Membership;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Xunit;

namespace Squirix.UnitTests.Internal.Cluster.Transport;

/// <summary>
/// Regression coverage for <see cref="ClientPool" /> gRPC channel reuse (issue #1).
/// </summary>
public sealed class ClientPoolChannelReuseTests
{
    private const string MeterName = "Squirix";
    private const string PoolSizeInstrumentName = "squirix_client_pool_size";
    private const string PoolPeerCountInstrumentName = "squirix_client_peer_count";

    /// <summary>
    /// Repeated lookups for the same node must return the same gRPC client instance.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ForNodeReusesSameClientAcrossManyLookups()
    {
        var peers = new[]
        {
            new Peer
            {
                NodeId = "node-a",
                Url = "http://127.0.0.1:6500",
            },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        var first = pool.ForNode("node-a");

        for (var i = 0; i < 256; i++)
            Assert.Same(first, pool.ForNode("node-a"));
    }

    /// <summary>
    /// Many ForNode lookups must not grow the pooled channel count beyond the configured peer set.
    /// </summary>
    /// <returns>A task representing the asynchronous unit test.</returns>
    [Fact]
    public async Task PoolSizeRemainsStableAfterManyForNodeLookups()
    {
        var poolSize = new ConcurrentBag<int>();
        var peerCount = new ConcurrentBag<int>();

        var measurements = new Dictionary<string, ConcurrentBag<int>>(StringComparer.Ordinal)
        {
            [PoolSizeInstrumentName] = poolSize,
            [PoolPeerCountInstrumentName] = peerCount,
        };

        using var listener = new MeterListener();
        listener.InstrumentPublished = static (instrument, listener) =>
        {
            if (instrument.Meter.Name != MeterName)
                return;

            if (instrument.Name is PoolSizeInstrumentName or PoolPeerCountInstrumentName)
                listener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) =>
        {
            if (measurements.TryGetValue(instrument.Name, out var bag))
                bag.Add(measurement);
        });

        listener.Start();

        var peers = new[]
        {
            new Peer { NodeId = "node-a", Url = "http://127.0.0.1:6501" },
            new Peer { NodeId = "node-b", Url = "http://127.0.0.1:6502" },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        var anchor = pool.ForNode("node-a");

        for (var i = 0; i < 256; i++)
        {
            _ = pool.ForNode(i % 2 == 0 ? "node-a" : "node-b");
            listener.RecordObservableInstruments();
        }

        Assert.Same(anchor, pool.ForNode("node-a"));
        Assert.Contains(2, poolSize);
        Assert.Contains(2, peerCount);
        Assert.DoesNotContain(poolSize, static size => size > 2);
    }
}
