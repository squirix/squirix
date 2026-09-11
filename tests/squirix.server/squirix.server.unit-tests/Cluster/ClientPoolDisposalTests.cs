using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Pool shutdown disposal resilience (issue #450 C2).</summary>
[Immutable]
public sealed class ClientPoolDisposalTests : DisposableServerUnitTestBase
{
    private const string PoolDisposalsTotalInstrumentName = "squirix_peer_pool_disposals_total";

    /// <summary>An unexpected policy disposal failure must not leak the remaining peers or channels.</summary>
    [Fact]
    public async Task DisposeContinuesAfterPolicyFailure()
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        var policies = new Dictionary<string, RecordingPolicy>(StringComparer.Ordinal)
        {
            ["n0"] = new(new InvalidOperationException("Unexpected policy disposal failure.")),
            ["n1"] = new(null),
            ["n2"] = new(null),
        };
        var pool = new ServerClientPool(BuildPeers(3), new ServerClientPoolArgs { PolicyFactory = nodeId => policies[nodeId] }, new ServerClientPoolMetrics(meter));
        await using (pool)
        {
            await pool.DisposeAsync();

            Assert.True(policies["n0"].Disposed);
            Assert.True(policies["n1"].Disposed);
            Assert.True(policies["n2"].Disposed);
            Assert.True(sink.HasEvent(PoolDisposalsTotalInstrumentName));
        }
    }

    private static ServerPeer[] BuildPeers(int count)
    {
        var peers = new ServerPeer[count];
        for (var i = 0; i < count; i++)
            peers[i] = new ServerPeer { NodeId = $"n{NodeInvariantIndexStrings.Format(i)}", Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", 6500 + i)) };

        return peers;
    }

    private sealed class RecordingPolicy : IServerCallPolicy
    {
        private readonly Exception? _disposeError;

        internal RecordingPolicy(Exception? disposeError)
        {
            _disposeError = disposeError;
        }

        internal bool Disposed { get; private set; }

        public void BeginDrain()
        {
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return _disposeError != null ? ValueTask.FromException(_disposeError) : ValueTask.CompletedTask;
        }

        public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException("RecordingPolicy does not execute calls.");
    }
}
