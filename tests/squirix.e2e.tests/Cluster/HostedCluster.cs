using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Cluster;

/// <summary>Lifecycle wrapper for a started Squirix test cluster (single- or multi-node).</summary>
[Immutable]
internal sealed class HostedCluster : IAsyncDisposable
{
    private static readonly string[] SingleNodeIds = ["nodeA"];

    private static readonly SemaphoreSlim StartupGate = new(ClusterStartupLimit.MaxConcurrentStartups, ClusterStartupLimit.MaxConcurrentStartups);
    private static readonly string[] ThreeNodeIds = ["nodeA", "nodeB", "nodeC"];
    private static readonly string[] TwoNodeIds = ["nodeA", "nodeB"];

    private readonly List<ISquirixClient> _clients = [];
    private readonly TestCluster<ClusterStartOptions> _cluster;
    private int _disposed;

    private HostedCluster(TestCluster<ClusterStartOptions> cluster)
    {
        _cluster = cluster;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        for (var i = _clients.Count - 1; i >= 0; i--)
            await _clients[i].DisposeAsync();

        _clients.Clear();
        await _cluster.DisposeAsync();
    }

    internal static ValueTask<HostedCluster> StartSingleNodeAsync(
        string? name = null,
        TestNodeSecurityOptions? security = null,
        bool persistence = false,
        TimeProvider? timeProvider = null,
        CancellationToken cancellationToken = default)
    {
        var options = new MultiNodeStartOptions { Security = security, TimeProvider = timeProvider };
        return StartAsync(SingleNodeIds, options, name, persistence, cancellationToken);
    }

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

    internal async ValueTask<ISquirixClient> ConnectClientAsync(string nodeId = "nodeA", CancellationToken cancellationToken = default)
    {
        var client = await LoopbackConnect.ConnectAsync(_cluster[nodeId].Uri, cancellationToken);
        _clients.Add(client);
        return client;
    }

    /// <summary>Gets a started node by identifier.</summary>
    /// <param name="nodeId">Node identifier.</param>
    /// <returns>The started test node host.</returns>
    internal ITestNodeHost GetNode(string nodeId) => _cluster[nodeId];

    internal Uri GetUri(string nodeId) => _cluster[nodeId].Uri;

    /// <summary>Stops and removes one HostedCluster node while leaving other nodes running.</summary>
    /// <param name="id">Node identifier to stop.</param>
    internal ValueTask StopNodeAsync(string id) => _cluster.StopNodeAsync(id);

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
        TimeProvider = startOptions.TimeProvider,
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

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Ownership of the shared identity and data directory transfers to the cluster, which disposes them.")]
    private static async ValueTask<HostedCluster> StartCoreAsync(
        string[] nodeIds,
        MultiNodeStartOptions? startOptions,
        string? testName,
        bool usePersistence,
        CancellationToken cancellationToken)
    {
        startOptions ??= new MultiNodeStartOptions();

        // Multi-node topologies share one ClusterIdentity material so peer trust anchors stay consistent.
        var identity = nodeIds.Length > 1 ? new ClusterIdentity() : null;
        var dir = usePersistence ? new TempDirectory("squirix-e2e", testName ?? "unknown") : null;
        var reserved = Array.Empty<HeldPort>();
        try
        {
            // Reserve one loopback port per node and keep them bound until each node binds. Because the
            // reserved ports stay marked as in-process reservations even after they are released for a node,
            // the pool will not hand the same port to a later caller, and cross-process slices are disjoint.
            // This closes the pool-level TOCTOU race; an unrelated third-party process could still grab a
            // briefly released port, which upstream probes already guard against.
            reserved = ListenPortPool.EndToEndTests.HoldPorts(nodeIds.Length);
            var topology = new ClusterNode[nodeIds.Length];
            for (var i = 0; i < nodeIds.Length; i++)
                topology[i] = new ClusterNode(nodeIds[i], reserved[i].HttpUri);

            var cluster = TestCluster<ClusterStartOptions>.Create(topology, identity: identity, dataDir: dir);
            _ = await cluster.StartAllAsync(
                nodeId => CreateNodeOptions(startOptions, nodeId, usePersistence, dir),
                (started, total) =>
                {
                    for (var i = started; i < total; i++)
                        reserved[i].Dispose();
                },
                cancellationToken).ConfigureAwait(false);

            return new HostedCluster(cluster);
        }
        catch
        {
            // TestCluster.Create never fails; a throw here means the reserved ports (and, for multi-node
            // topologies, the identity/data directory) were never handed to a cluster to own, so releasing
            // them is this method's responsibility. StartAllAsync already disposed the cluster and released
            // its own unstarted-node ports on a node-start failure.
            identity?.Dispose();
            dir?.Dispose();

            for (var i = 0; i < reserved.Length; i++)
                reserved[i].Dispose();

            throw;
        }
    }
}
