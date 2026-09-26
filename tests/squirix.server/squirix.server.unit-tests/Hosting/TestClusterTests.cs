using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Lifecycle coverage for <see cref="TestCluster{TOptions}" />: start/stop/dispose resilience against partial failures.</summary>
public sealed class TestClusterTests
{
    /// <summary>Stopping an identifier outside the cluster topology fails loudly instead of silently doing nothing.</summary>
    [Test]
    public async Task StopNodeAsyncThrowsForUnknownNodeId()
    {
        await using var cluster = CreateCluster(out _, "n1");

        var ex = await NodeAsyncAssert.ThrowsAsync<ArgumentException>(cluster.StopNodeAsync("unknown"));
        _ = await Assert.That(ex.Message).Contains("not part of the cluster topology", StringComparison.Ordinal);
    }

    /// <summary>Stopping a topology entry that was never started, or was already stopped, is a no-op.</summary>
    [Test]
    public async Task StopUnstartedTopologyNodeIsNoOp()
    {
        await using var cluster = CreateCluster(out _, "n1");

        await cluster.StopNodeAsync("n1");
    }

    /// <summary>Every node still gets a shutdown attempt even when an earlier node's shutdown fails, and the failures are aggregated.</summary>
    [Test]
    public async Task DisposeAggregatesShutdownFailures()
    {
        await using var cluster = CreateCluster(out var hosts, "n1", "n2");
        hosts["n1"].FailShutdown = true;
        hosts["n2"].FailShutdown = true;

        _ = await cluster.StartAllAsync(cancellationToken: CancellationToken.None);

        var ex = await NodeAsyncAssert.ThrowsAsync<AggregateException>(cluster.DisposeAsync());
        _ = await Assert.That(ex.InnerExceptions.Count).IsEqualTo(2);
        _ = await Assert.That(hosts["n1"].ShutdownCallCount).IsEqualTo(1);
        _ = await Assert.That(hosts["n2"].ShutdownCallCount).IsEqualTo(1);
    }

    /// <summary>A node-start failure rolls the whole cluster back and releases only the topology indices that never started.</summary>
    [Test]
    public async Task StartAllReleasesUnstartedOnFailure()
    {
        await using var cluster = CreateCluster(out var hosts, "n1", "n2", "n3");
        hosts["n2"].FailStart = true;
        List<int> released = [];

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<ClusterStartOptions>>(
            cluster.StartAllAsync(releaseUnstarted: released.Add, cancellationToken: CancellationToken.None));

        _ = await Assert.That(ex.Message).Contains("n2", StringComparison.Ordinal);
        _ = await Assert.That(released.Count).IsEqualTo(2);
        _ = await Assert.That(released[0]).IsEqualTo(1);
        _ = await Assert.That(released[1]).IsEqualTo(2);
        _ = await Assert.That(cluster.StartedCount).IsEqualTo(0);
        _ = await Assert.That(hosts["n1"].ShutdownCallCount).IsEqualTo(1);
    }

    /// <summary>A rejected start (the node is already running) must not overwrite the options a later restart falls back to.</summary>
    [Test]
    public async Task StartNodeKeepsOptionsAfterRejectedStart()
    {
        await using var cluster = CreateCluster(out var hosts, "n1");
        var first = new ClusterStartOptions { ReplicaCount = 2 };
        _ = await cluster.StartNodeAsync("n1", first, CancellationToken.None);

        var rejected = new ClusterStartOptions { ReplicaCount = 5 };
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync("n1", rejected, CancellationToken.None));

        await cluster.StopNodeAsync("n1");
        _ = await cluster.RestartNodeAsync("n1", cancellationToken: CancellationToken.None);

        _ = await Assert.That(hosts["n1"].LastOptions).IsSameReferenceAs(first);
    }

    /// <summary>The divergent start overload rejects a node identifier that is already running.</summary>
    [Test]
    public async Task DivergentStartThrowsWhenAlreadyRunning()
    {
        await using var cluster = CreateCluster(out _, "n1");
        _ = await cluster.StartNodeAsync("n1", cancellationToken: CancellationToken.None);

        var node = new ClusterNode("n1", new Uri("https://127.0.0.1:1001"));
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync(node, [node], cancellationToken: CancellationToken.None));

        _ = await Assert.That(ex.Message).Contains("already running", StringComparison.Ordinal);
    }

    /// <summary>Starting a node on an already-disposed cluster fails loudly instead of silently resurrecting the cluster.</summary>
    [Test]
    public async Task StartNodeAsyncThrowsAfterDispose()
    {
        var cluster = CreateCluster(out _, "n1");
        await cluster.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, ITestNodeHost>(cluster.StartNodeAsync("n1", cancellationToken: CancellationToken.None));
    }

    /// <summary>Indexing a node that was never started, or was already stopped, fails loudly instead of returning a stale handle.</summary>
    [Test]
    public async Task IndexerThrowsForUnknownNode()
    {
        await using var cluster = CreateCluster(out _, "n1");

        _ = NodeExceptionAssert.For<KeyNotFoundException>().Throws(cluster, static c => _ = c["n1"]);
    }

    /// <summary>Explicit restart options win over the last successful options and become the new fallback for the next restart.</summary>
    [Test]
    public async Task RestartUsesExplicitOptionsOverFallback()
    {
        await using var cluster = CreateCluster(out var hosts, "n1");
        var first = new ClusterStartOptions { ReplicaCount = 2 };
        _ = await cluster.StartNodeAsync("n1", first, CancellationToken.None);

        var second = new ClusterStartOptions { ReplicaCount = 3 };
        _ = await cluster.RestartNodeAsync("n1", second, CancellationToken.None);
        _ = await Assert.That(hosts["n1"].LastOptions).IsSameReferenceAs(second);

        // The explicit restart options become the new fallback for a later restart with no override.
        _ = await cluster.RestartNodeAsync("n1", cancellationToken: CancellationToken.None);
        _ = await Assert.That(hosts["n1"].LastOptions).IsSameReferenceAs(second);
    }

    /// <summary>StartAllAsync rejects a non-empty cluster without tearing down nodes the caller already started.</summary>
    [Test]
    public async Task StartAllThrowsWhenNodesAlreadyRunning()
    {
        await using var cluster = CreateCluster(out var hosts, "n1", "n2");
        _ = await cluster.StartNodeAsync("n1", cancellationToken: CancellationToken.None);

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<ClusterStartOptions>>(cluster.StartAllAsync(cancellationToken: CancellationToken.None));

        _ = await Assert.That(ex.Message).Contains("empty cluster", StringComparison.Ordinal);
        _ = await Assert.That(cluster.StartedCount).IsEqualTo(1);
        _ = await Assert.That(hosts["n1"].ShutdownCallCount).IsEqualTo(0);
    }

    /// <summary>Disposal waits for an in-flight node stop before releasing the shared data directory.</summary>
    [Test]
    public async Task DisposeWaitsForInFlightStop()
    {
        var dataDir = new TempDirectory("squirix-test-cluster-stop-race", string.Empty);
        string dataPath = dataDir;
        var cluster = CreateCluster(out var hosts, dataDir, "n1");
        _ = await cluster.StartNodeAsync("n1", cancellationToken: CancellationToken.None);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        hosts["n1"].ShutdownGate = gate.Task;

        // Both calls run synchronously up to their first incomplete await, so the stop is registered before disposal starts.
        var stop = StopAsync(cluster, "n1");
        var dispose = cluster.DisposeAsync().AsTask();

        _ = await Assert.That(stop.IsCompleted).IsFalse();
        _ = await Assert.That(dispose.IsCompleted).IsFalse().Because("Disposal must wait for the in-flight stop.");
        _ = await Assert.That(Directory.Exists(dataPath)).IsTrue().Because("The data directory must outlive the in-flight stop.");

        gate.SetResult();
        await stop;
        await dispose;

        _ = await Assert.That(Directory.Exists(dataPath)).IsFalse();
        _ = await Assert.That(hosts["n1"].ShutdownCallCount).IsEqualTo(1);
    }

    /// <summary>Disposal releases the held listen port of a topology entry that never started.</summary>
    [Test]
    public async Task DisposeReleasesUnstartedHeldPort()
    {
        var uri = ListenPortPool.ServerUnitTests.HoldHttpUri();
        var cluster = TestCluster<ClusterStartOptions>.Create(
            new ClusterNode("n1", uri),
            static (node, _, _, _) => ValueTask.FromException<ITestNodeHost>(new InvalidOperationException($"'{node.NodeId}' must not start.")));

        await cluster.DisposeAsync();

        // Throws while the reservation still holds the port bound with exclusive address use.
        BindExclusively(uri.Port);
    }

    private static void BindExclusively(int port)
    {
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Server.ExclusiveAddressUse = true;
        listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, false);
        listener.Start();
        listener.Stop();
    }

    private static Task StopAsync(TestCluster<ClusterStartOptions> cluster, string nodeId) => cluster.StopNodeAsync(nodeId).AsTask();

    private static TestCluster<ClusterStartOptions> CreateCluster(out Dictionary<string, FakeTestNodeHost> hosts, params ReadOnlySpan<string> nodeIds) =>
        CreateCluster(out hosts, null, nodeIds);

    private static TestCluster<ClusterStartOptions> CreateCluster(out Dictionary<string, FakeTestNodeHost> hosts, TempDirectory? dataDir, params ReadOnlySpan<string> nodeIds)
    {
        var topology = new ClusterNode[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            topology[i] = new ClusterNode(nodeIds[i], new Uri($"https://127.0.0.1:{2000 + i}"));

        var hostMap = new Dictionary<string, FakeTestNodeHost>(StringComparer.Ordinal);
        for (var i = 0; i < topology.Length; i++)
            hostMap[nodeIds[i]] = new FakeTestNodeHost(topology[i].Uri);
        hosts = hostMap;

        return TestCluster<ClusterStartOptions>.Create(
            topology,
            (node, _, options, _) =>
            {
                var host = hostMap[node.NodeId];
                if (host.FailStart)
                    throw new InvalidOperationException($"Simulated start failure for '{node.NodeId}'.");

                host.LastOptions = options;
                return ValueTask.FromResult<ITestNodeHost>(host);
            },
            dataDir: dataDir);
    }

    private sealed class FakeTestNodeHost : ITestNodeHost
    {
        private int _shutdownCallCount;

        internal FakeTestNodeHost(Uri uri)
        {
            Uri = uri;
        }

        public string DataDir => string.Empty;

        public bool HasInterNodeMtlsListener => false;

        public bool PersistenceEnabled => false;

        public IServiceProvider Services { get; } = new EmptyServiceProvider();

        public Uri Uri { get; }

        internal bool FailShutdown { get; set; }

        internal bool FailStart { get; set; }

        internal ClusterStartOptions? LastOptions { get; set; }

        internal Task ShutdownGate { get; set; } = Task.CompletedTask;

        internal int ShutdownCallCount => _shutdownCallCount;

        public ValueTask AbruptShutdownAsync() => ShutdownAsync();

        public ValueTask ShutdownAsync()
        {
            _ = Interlocked.Increment(ref _shutdownCallCount);
            return FailShutdown ? ValueTask.FromException(new InvalidOperationException("Simulated shutdown failure.")) : new ValueTask(ShutdownGate);
        }

        public ValueTask DisposeAsync() => ShutdownAsync();
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }
}
