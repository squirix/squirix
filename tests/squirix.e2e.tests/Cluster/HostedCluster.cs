using System;
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
    private static readonly string[] TwoNodeIds = ["nodeA", "nodeB"];

    private readonly List<ISquirixClient> _clients = [];
    private readonly TempDirectory? _dataDir;
    private readonly ClusterTls? _mtls;
    private readonly Dictionary<string, TestNode> _nodes;
    private int _disposed;

    private HostedCluster(Dictionary<string, TestNode> nodes, ClusterTls? mtls, TempDirectory? dataDir)
    {
        _nodes = nodes;
        _mtls = mtls;
        _dataDir = dataDir;
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

        // Pre-allocate every listen URI so each node advertises the same peer topology at startup.
        var uris = new Dictionary<string, Uri>(StringComparer.Ordinal);
        for (var i = 0; i < nodeIds.Length; i++)
            uris[nodeIds[i]] = ListenPortPool.EndToEndTests.NextHttpUri();

        var topology = new (string NodeId, Uri Uri)[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            topology[i] = (nodeIds[i], uris[nodeIds[i]]);

        var nodes = new Dictionary<string, TestNode>(StringComparer.Ordinal);

        // Multi-node topologies share one ClusterTls material so peer trust anchors stay consistent.
        var mtls = nodeIds.Length > 1 ? new ClusterTls() : null;
        var dataDir = usePersistence ? new TempDirectory("squirix-e2e", testName ?? "unknown") : null;
        try
        {
            for (var i = 0; i < nodeIds.Length; i++)
            {
                var nodeId = nodeIds[i];
                var hostOptions = new TestNodeHostStartOptions
                {
                    DataDir = usePersistence ? BuildDataDir(dataDir!, nodeId) : null,
                    Security = startOptions.Security,
                    MtlsProfile = startOptions.GetProfile(nodeId),
                    TimeProvider = startOptions.TimeProvider,
                };
                nodes[nodeId] = new TestNode(await TestNodeHostFactory.StartNodeAsync(nodeId, uris[nodeId], topology, hostOptions, mtls, cancellationToken));
            }

            return new HostedCluster(nodes, mtls, dataDir);
        }
        catch (Exception ex) when (ex is InvalidOperationException or IOException)
        {
            // Startup failures (configuration or I/O) must dispose already-started nodes before rethrowing.
            foreach (var node in nodes.Values)
                await node.DisposeAsync();

            mtls?.Dispose();
            dataDir?.Dispose();
            throw;
        }
    }

    /// <summary>Represents a started test node.</summary>
    [Immutable]
    private sealed class TestNode : IAsyncDisposable
    {
        private readonly TestNodeHost _host;

        internal TestNode(TestNodeHost host)
        {
            _host = host ?? throw new ArgumentNullException(nameof(host));
        }

        internal Uri Uri => _host.Uri;

        public ValueTask DisposeAsync() => _host.DisposeAsync();
    }
}
