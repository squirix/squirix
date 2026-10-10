using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Lifecycle wrapper for a started Squirix test cluster (single- or multi-node).</summary>
[Immutable]
internal sealed class HostedCluster : IAsyncDisposable
{
    private static readonly string[] SingleNodeIds = ["nodeA"];

    private static readonly string[] FiveNodeIds = ["nodeA", "nodeB", "nodeC", "nodeD", "nodeE"];

    private static readonly SemaphoreSlim StartupGate = new(ClusterStartupLimit.MaxConcurrentStartups, ClusterStartupLimit.MaxConcurrentStartups);
    private static readonly string[] ThreeNodeIds = ["nodeA", "nodeB", "nodeC"];
    private static readonly string[] TwoNodeIds = ["nodeA", "nodeB"];

    private readonly List<ISquirixClient> _clients = [];
    private readonly Dictionary<string, ISquirixClient> _nodeClients = [with(StringComparer.Ordinal)];
    private int _disposed;

    private HostedCluster(TestCluster<ClusterStartOptions> cluster)
    {
        Cluster = cluster;
    }

    /// <summary>Gets the underlying test cluster, for the testkit probes that read node state.</summary>
    internal TestCluster<ClusterStartOptions> Cluster { get; }

    /// <summary>Gets the number of clients <see cref="ConnectClientAsync" /> opened and the cluster still holds.</summary>
    internal int ClientCount => _clients.Count;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        for (var i = _clients.Count - 1; i >= 0; i--)
            await _clients[i].DisposeAsync();

        _clients.Clear();
        foreach (var client in _nodeClients.Values)
            await client.DisposeAsync();

        _nodeClients.Clear();
        await Cluster.DisposeAsync();
    }

    internal static ValueTask<HostedCluster> StartSingleNodeAsync(
        string? name = null,
        TestNodeSecurityOptions? security = null,
        bool persistence = false,
        TimeProvider? timeProvider = null,
        Action<string, IServiceCollection>? configure = null,
        CancellationToken cancellationToken = default)
    {
        var options = new MultiNodeStartOptions { Security = security, TimeProvider = timeProvider, ServicesConfigure = configure };
        return StartAsync(SingleNodeIds, options, name, persistence, cancellationToken);
    }

    /// <summary>Starts a five-node cluster for RF=5 and wide-topology failover scenarios.</summary>
    /// <param name="testName">Label used when creating a persistence temp directory.</param>
    /// <param name="options">Replica count, failover, clock, fabric, security, and mTLS profile overrides.</param>
    /// <param name="usePersistence">When <see langword="true" />, each node gets an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A hosted cluster owning the started nodes.</returns>
    internal static ValueTask<HostedCluster> StartFiveNodeAsync(
        string? testName = null,
        MultiNodeStartOptions? options = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(FiveNodeIds, options, testName, usePersistence, cancellationToken);

    /// <summary>Starts a three-node cluster for RF=3 quorum scenarios.</summary>
    /// <param name="testName">Label used when creating a persistence temp directory.</param>
    /// <param name="options">Replica count, security, and mTLS profile overrides.</param>
    /// <param name="usePersistence">When <see langword="true" />, each node gets an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A hosted cluster owning the started nodes.</returns>
    internal static ValueTask<HostedCluster> StartThreeNodeAsync(
        string? testName = null,
        MultiNodeStartOptions? options = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(ThreeNodeIds, options, testName, usePersistence, cancellationToken);

    internal static ValueTask<HostedCluster> StartTwoNodeAsync(
        string? testName = null,
        TestNodeSecurityOptions? security = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartTwoNodeAsync(new MultiNodeStartOptions { Security = security }, testName, usePersistence, cancellationToken);

    internal static ValueTask<HostedCluster> StartTwoNodeAsync(
        MultiNodeStartOptions? options,
        string? testName = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(TwoNodeIds, options, testName, usePersistence, cancellationToken);

    /// <summary>Shuts one running node down abruptly, without a graceful drain, leaving it registered until it is stopped or restarted.</summary>
    /// <param name="nodeId">Node identifier.</param>
    /// <returns>A task that completes when the node has shut down.</returns>
    internal ValueTask AbruptShutdownNodeAsync(string nodeId) => Cluster[nodeId].AbruptShutdownAsync();

    internal async ValueTask<ISquirixClient> ConnectClientAsync(string nodeId = "nodeA", CancellationToken cancellationToken = default)
    {
        var client = await LoopbackConnect.ConnectAsync(Cluster[nodeId].Uri, cancellationToken);
        _clients.Add(client);
        return client;
    }

    /// <summary>Gets a typed cache facade through the node's cluster-owned client, connecting it on first use.</summary>
    /// <remarks>
    /// The client belongs to the node: <see cref="StopNodeAsync" /> and <see cref="RestartNodeAsync" /> dispose it
    /// before the node stops, and the next call reconnects to the restarted node.
    /// </remarks>
    /// <typeparam name="T">Cached value type.</typeparam>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="nodeId">Node identifier the client connects to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The cache facade.</returns>
    internal async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, string nodeId = "nodeA", CancellationToken cancellationToken = default)
    {
        if (_nodeClients.TryGetValue(nodeId, out var client))
            return await client.GetCacheAsync<T>(cacheName, cancellationToken);

        client = await LoopbackConnect.ConnectAsync(Cluster[nodeId].Uri, cancellationToken);
        _nodeClients[nodeId] = client;
        return await client.GetCacheAsync<T>(cacheName, cancellationToken);
    }

    /// <summary>Disposes the clients <see cref="ConnectClientAsync" /> opened after the first given number of them, so a long test does not hold one pair per step.</summary>
    /// <param name="keep">The number of the oldest clients to keep, as <see cref="ClientCount" /> reported it earlier.</param>
    /// <returns>A task that completes once the newer clients are disposed.</returns>
    internal async ValueTask DisposeClientsAfterAsync(int keep)
    {
        for (var i = _clients.Count - 1; i >= keep; i--)
        {
            var client = _clients[i];
            _clients.RemoveAt(i);
            await client.DisposeAsync();
        }
    }

    internal Uri GetUri(string nodeId) => Cluster[nodeId].Uri;

    /// <summary>Gets a value indicating whether a running node opened the internode mTLS listener.</summary>
    /// <param name="nodeId">Node identifier.</param>
    /// <returns><see langword="true" /> when the node listens for internode mTLS traffic.</returns>
    internal bool HasInterNodeMtlsListener(string nodeId) => Cluster[nodeId].HasInterNodeMtlsListener;

    /// <summary>Stops one node and starts it again on the same data directory and listen address.</summary>
    /// <param name="id">Node identifier to restart.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the node is running again.</returns>
    internal async ValueTask RestartNodeAsync(string id, CancellationToken cancellationToken = default)
    {
        // The stop must complete before the restart: a canceled wait would leave the previous host
        // shutting down while the new one binds the same URI and data directory.
        await StopNodeAsync(id);
        cancellationToken.ThrowIfCancellationRequested();
        _ = await Cluster.RestartNodeAsync(id, null, cancellationToken);
    }

    /// <summary>Stops and removes one HostedCluster node while leaving other nodes running.</summary>
    /// <remarks>The node's client from <see cref="GetCacheAsync{T}" /> is disposed before the node stops.</remarks>
    /// <param name="id">Node identifier to stop.</param>
    /// <returns>A task that completes when the node is stopped.</returns>
    internal async ValueTask StopNodeAsync(string id)
    {
        try
        {
            if (_nodeClients.Remove(id, out var client))
                await client.DisposeAsync();
        }
        finally
        {
            await Cluster.StopNodeAsync(id);
        }
    }

    private static string BuildDataDir(TempDirectory dir, string nodeId)
    {
        var path = NodePathKit.Combine(dir, nodeId);
        Directory.CreateDirectory(path);
        return path;
    }

    private static ClusterStartOptions CreateNodeOptions(MultiNodeStartOptions startOptions, string nodeId, bool usePersistence, TempDirectory? dataDir) => new()
    {
        DataDir = usePersistence ? BuildDataDir(dataDir!, nodeId) : null,
        ReplicaCount = startOptions.ReplicaCount,
        Security = startOptions.Security,
        MtlsProfile = startOptions.GetProfile(nodeId),
        TimeProvider = startOptions.ClockFor(nodeId),
        AutomaticFailoverEnabled = startOptions.Failover,
        QuorumReadsEnabled = startOptions.Failover,
        ElectionTiming = startOptions.ElectionTiming,
        PartitionFabric = startOptions.PartitionFabric,
        ServicesConfigure = startOptions.ServicesConfigure == null ? null : services => startOptions.ServicesConfigure(nodeId, services),
    };

    /// <summary>Allocates listen URIs, starts each node with a shared topology, and rolls back on partial failure.</summary>
    /// <param name="nodeIds">Ordered node identifiers to start.</param>
    /// <param name="startOptions">Optional security and mTLS profile overrides.</param>
    /// <param name="testName">Label used when creating a persistence temp directory.</param>
    /// <param name="usePersistence">When <see langword="true" />, each node gets an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A hosted cluster owning the started nodes.</returns>
    private static async ValueTask<HostedCluster> StartAsync(
        string[] nodeIds,
        MultiNodeStartOptions? startOptions,
        string? testName,
        bool usePersistence,
        CancellationToken cancellationToken = default)
    {
        startOptions?.EnsureElectionClocks(nodeIds);

        // Cap concurrent cluster startups process-wide: cold Debug host builds and RSA key
        // generation are CPU-heavy, and a thundering herd at session start pushes single
        // startups past the fixture budgets. Queued starters observe cancellation, and tests
        // themselves stay fully parallel once their cluster is up.
        await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await StartCoreAsync(nodeIds, startOptions, testName, usePersistence, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = StartupGate.Release();
        }
    }

    private static async ValueTask<HostedCluster> StartCoreAsync(
        string[] nodeIds,
        MultiNodeStartOptions? startOptions,
        string? testName,
        bool usePersistence,
        CancellationToken cancellationToken)
    {
        startOptions ??= new MultiNodeStartOptions();

        ClusterIdentity? identity = null;
        TestCluster<ClusterStartOptions>? cluster = null;
        var reserved = Array.Empty<HeldPort>();
        try
        {
            // Multi-node topologies share one ClusterIdentity material so peer trust anchors stay consistent.
            identity = nodeIds.Length > 1 ? new ClusterIdentity() : null;

            // Reserve one loopback port per node and keep them bound until each node binds. Because the
            // reserved ports stay marked as in-process reservations even after they are released for a node,
            // the pool will not hand the same port to a later caller, and cross-process slices are disjoint.
            // This closes the pool-level TOCTOU race; an unrelated third-party process could still grab a
            // briefly released port, which upstream probes already guard against.
            reserved = ListenPortPool.EndToEndTests.HoldPorts(nodeIds.Length);
            var topology = new ClusterNode[nodeIds.Length];
            for (var i = 0; i < nodeIds.Length; i++)
                topology[i] = new ClusterNode(nodeIds[i], reserved[i].HttpUri);

            cluster = TestCluster<ClusterStartOptions>.Create(topology, identity: identity, dir: usePersistence ? $"squirix-e2e-{testName ?? "unknown"}" : null);
            identity = null;
            _ = await cluster.StartAllAsync(nodeId => CreateNodeOptions(startOptions, nodeId, usePersistence, cluster.DataDir), i => reserved[i].Dispose(), cancellationToken)
                             .ConfigureAwait(false);

            var hosted = new HostedCluster(cluster);
            cluster = null;
            return hosted;
        }
        finally
        {
            // Create takes ownership of identity immediately (nulled above once it does), and
            // StartAllAsync's own rollback disposes the cluster (and its data directory) on a
            // node-start failure, so only a failure before Create ever ran leaves identity or
            // cluster still set here.
            identity?.Dispose();
            if (cluster != null)
                await cluster.DisposeAsync().ConfigureAwait(false);

            for (var i = 0; i < reserved.Length; i++)
                reserved[i].Dispose();
        }
    }
}
