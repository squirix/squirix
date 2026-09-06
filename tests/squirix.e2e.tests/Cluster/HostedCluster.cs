using System;
using System.Collections.Frozen;
using System.Collections.Generic;
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
    private static readonly string[] ThreeNodeIds = ["nodeA", "nodeB", "nodeC"];
    private static readonly string[] TwoNodeIds = ["nodeA", "nodeB"];

    private readonly List<ISquirixClient> _clients = [];
    private readonly TempDirectory? _dataDir;
    private readonly ClusterTls? _mtls;
    private readonly Dictionary<string, TestNode> _nodes;
    private readonly TwoNodeStartOptions _startOptions;
    private readonly FrozenDictionary<string, Uri> _uris;
    private readonly bool _usePersistence;
    private int _disposed;

    private HostedCluster(
        Dictionary<string, TestNode> nodes,
        ClusterTls? mtls,
        TempDirectory? dataDir,
        TwoNodeStartOptions startOptions,
        FrozenDictionary<string, Uri> uris,
        bool usePersistence)
    {
        _nodes = nodes;
        _mtls = mtls;
        _dataDir = dataDir;
        _startOptions = startOptions;
        _uris = uris;
        _usePersistence = usePersistence;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        for (var i = _clients.Count - 1; i >= 0; i--)
            await _clients[i].DisposeAsync();

        foreach (var node in _nodes.Values)
            await node.DisposeAsync();

        _mtls?.Dispose();
        _dataDir?.Dispose();
    }

    internal static ValueTask<HostedCluster> StartSingleNodeAsync(
        string? name = null,
        TestNodeSecurityOptions? security = null,
        bool persistence = false,
        TimeProvider? timeProvider = null,
        CancellationToken cancellationToken = default)
    {
        var options = new TwoNodeStartOptions { Security = security, TimeProvider = timeProvider };
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
        TwoNodeStartOptions? options = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(ThreeNodeIds, options, testName, usePersistence, cancellationToken);

    internal static ValueTask<HostedCluster> StartTwoNodeAsync(
        string? testName = null,
        TestNodeSecurityOptions? security = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartTwoNodeAsync(new TwoNodeStartOptions { Security = security }, testName, usePersistence, cancellationToken);

    internal static ValueTask<HostedCluster> StartTwoNodeAsync(
        TwoNodeStartOptions? options,
        string? testName = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(TwoNodeIds, options, testName, usePersistence, cancellationToken);

    internal async ValueTask<ISquirixClient> ConnectClientAsync(string nodeId = "nodeA", CancellationToken cancellationToken = default)
    {
        var uri = _nodes[nodeId].Uri;
        var client = await LoopbackConnect.ConnectAsync(uri, cancellationToken);
        _clients.Add(client);
        return client;
    }

    internal Uri GetUri(string nodeId) => _nodes[nodeId].Uri;

    /// <summary>Stops and removes one HostedCluster node while leaving other nodes running.</summary>
    /// <param name="nodeId">Node identifier to stop.</param>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="nodeId" /> is not a running node.</exception>
    internal ValueTask StopNodeAsync(string nodeId)
    {
        if (!_nodes.Remove(nodeId, out var node))
            throw new InvalidOperationException("Requested node is not running.");

        return node.DisposeAsync();
    }

    private static string BuildDataDir(TempDirectory clusterRoot, string nodeId)
    {
        var path = NodePathKit.Combine(clusterRoot.Path, nodeId);
        Directory.CreateDirectory(path);
        return path;
    }

    /// <summary>Allocates listen URIs, starts each node with a shared topology, and rolls back on partial failure.</summary>
    /// <param name="nodeIds">Ordered node identifiers to start.</param>
    /// <param name="startOptions">Optional security and mTLS profile overrides.</param>
    /// <param name="testName">Label used when creating a persistence temp directory.</param>
    /// <param name="usePersistence">When <see langword="true" />, each node gets an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A hosted cluster owning the started nodes.</returns>
    private static async ValueTask<HostedCluster> StartAsync(
        string[] nodeIds,
        TwoNodeStartOptions? startOptions,
        string? testName,
        bool usePersistence,
        CancellationToken cancellationToken = default)
    {
        startOptions ??= new TwoNodeStartOptions();

        var pool = ListenPortPool.EndToEndTests;
        var nodes = new Dictionary<string, TestNode>(StringComparer.Ordinal);

        // Multi-node topologies share one ClusterTls material so peer trust anchors stay consistent.
        var mtls = nodeIds.Length > 1 ? new ClusterTls() : null;
        var dataDir = usePersistence ? new TempDirectory("squirix-e2e", testName ?? "unknown") : null;
        var reserved = Array.Empty<int>();
        try
        {
            // Reserve one loopback port per node and keep them bound until each node binds. Because the
            // reserved ports stay marked as in-process reservations even after they are released for a node,
            // the pool will not hand the same port to a later caller, and cross-process slices are disjoint.
            // This closes the pool-level TOCTOU race; an unrelated third-party process could still grab a
            // briefly released port, which upstream probes already guard against.
            reserved = pool.AllocateRange(nodeIds.Length);
            var uris = new Dictionary<string, Uri>(StringComparer.Ordinal);
            for (var i = 0; i < nodeIds.Length; i++)
                uris[nodeIds[i]] = new Uri($"https://127.0.0.1:{reserved[i]}", UriKind.Absolute);

            var topology = new (string NodeId, Uri Uri)[nodeIds.Length];
            for (var i = 0; i < nodeIds.Length; i++)
                topology[i] = (nodeIds[i], uris[nodeIds[i]]);

            var cluster = new HostedCluster(nodes, mtls, dataDir, startOptions, uris.ToFrozenDictionary(StringComparer.Ordinal), usePersistence);
            for (var i = 0; i < nodeIds.Length; i++)
            {
                var nodeId = nodeIds[i];

                // Release this node's held port so Kestrel can bind it, then start the node immediately.
                pool.ReleasePort(reserved[i]);
                nodes[nodeId] = new TestNode(await cluster.StartOneAsync(nodeId, topology, cancellationToken).ConfigureAwait(false));
            }

            return cluster;
        }
        catch
        {
            // Release any reserved ports that were never handed to a started node, then dispose all
            // already-started nodes before rethrowing. This runs for every failure, including startup
            // exceptions and cancellation, so held ports never leak for the process lifetime.
            for (var i = nodes.Count; i < reserved.Length; i++)
                pool.ReleasePort(reserved[i]);

            foreach (var node in nodes.Values)
                await node.DisposeAsync();

            mtls?.Dispose();
            dataDir?.Dispose();
            throw;
        }
    }

    private ValueTask<TestNodeHost> StartOneAsync(string nodeId, (string NodeId, Uri Uri)[] topology, CancellationToken cancellationToken)
    {
        var hostOptions = new TestNodeHostStartOptions
        {
            DataDir = _usePersistence ? BuildDataDir(_dataDir!, nodeId) : null,
            ReplicaCount = _startOptions.ReplicaCount,
            Security = _startOptions.Security,
            MtlsProfile = _startOptions.GetProfile(nodeId),
            TimeProvider = _startOptions.TimeProvider,
        };

        return TestNodeHostFactory.StartNodeAsync(nodeId, _uris[nodeId], topology, hostOptions, _mtls, cancellationToken);
    }

    /// <summary>Represents a started test node.</summary>
    [Immutable]
    private sealed class TestNode : IAsyncDisposable
    {
        private readonly TestNodeHost _host;

        internal TestNode(TestNodeHost host)
        {
            ArgumentNullException.ThrowIfNull(host);
            _host = host;
        }

        internal Uri Uri => _host.Uri;

        public ValueTask DisposeAsync() => _host.DisposeAsync();
    }
}
