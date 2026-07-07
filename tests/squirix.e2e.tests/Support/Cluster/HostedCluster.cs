using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.E2ETests.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Support.Cluster;

/// <summary>Lifecycle wrapper for a started Squirix test cluster (single- or multi-node).</summary>
internal sealed class HostedCluster : IAsyncDisposable
{
    private readonly List<ISquirixClient> _clients = [];
    private readonly TempDirectory? _dataDir;
    private readonly MtlsTestContext? _mtls;
    private readonly Dictionary<string, TestNode> _nodes;
    private int _disposed;

    private HostedCluster(Dictionary<string, TestNode> nodes, MtlsTestContext? mtls, TempDirectory? dataDir)
    {
        _nodes = nodes;
        _mtls = mtls;
        _dataDir = dataDir;
    }

    public static ValueTask<HostedCluster> StartTwoNodeAsync(
        string? testName = null,
        TestNodeSecurityOptions? security = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartTwoNodeAsync(new TwoNodeStartOptions { Security = security }, testName, usePersistence, cancellationToken);

    public static ValueTask<HostedCluster> StartTwoNodeAsync(
        TwoNodeStartOptions? options,
        string? testName = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(["nodeA", "nodeB"], options, testName, usePersistence, cancellationToken);

    public async ValueTask<ISquirixClient> ConnectClientAsync(string nodeId = "nodeA", CancellationToken cancellationToken = default)
    {
        var uri = _nodes[nodeId].Uri;
        var client = await LoopbackConnect.ConnectAsync(uri, cancellationToken);
        _clients.Add(client);
        return client;
    }

    public Uri GetUri(string nodeId) => _nodes[nodeId].Uri;

    /// <summary>Stops and removes one HostedCluster node while leaving other nodes running.</summary>
    /// <param name="nodeId">Node identifier to stop.</param>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="nodeId" /> is not a running node.</exception>
    public ValueTask StopNodeAsync(string nodeId)
    {
        if (!_nodes.Remove(nodeId, out var node))
            throw new InvalidOperationException($"Node '{nodeId}' is not running.");

        return node.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        for (var i = _clients.Count - 1; i >= 0; i--)
            await _clients[i].DisposeAsync();

        foreach (var node in _nodes.Values)
            await node.DisposeAsync();

        _mtls?.Dispose();
        _dataDir?.Dispose();
    }

    internal static ValueTask<HostedCluster> StartSingleNodeAsync(
        string? testName = null,
        TestNodeSecurityOptions? security = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(["nodeA"], new TwoNodeStartOptions { Security = security }, testName, usePersistence, cancellationToken);

    private static string BuildDataDir(TempDirectory clusterRoot, string nodeId)
    {
        var path = PathKit.Combine(clusterRoot.Path, nodeId);
        DirectoryKit.CreateDirectory(path);
        return path;
    }

    private static async ValueTask<HostedCluster> StartAsync(
        string[] nodeIds,
        TwoNodeStartOptions? startOptions,
        string? testName,
        bool usePersistence,
        CancellationToken cancellationToken = default)
    {
        startOptions ??= new TwoNodeStartOptions();
        var uris = new Dictionary<string, Uri>(StringComparer.Ordinal);
        for (var i = 0; i < nodeIds.Length; i++)
            uris[nodeIds[i]] = ListenPortPool.EndToEndTests.NextHttpUri();

        var topology = new (string NodeId, Uri Uri)[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            topology[i] = (nodeIds[i], uris[nodeIds[i]]);

        var nodes = new Dictionary<string, TestNode>(StringComparer.Ordinal);
        var mtls = nodeIds.Length > 1 ? new MtlsTestContext() : null;
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
                    Mtls = mtls,
                    MtlsProfile = startOptions.GetProfile(nodeId),
                };
                nodes[nodeId] = new TestNode(await TestNodeHostFactory.StartNodeAsync(nodeId, uris[nodeId], topology, hostOptions, cancellationToken));
            }

            return new HostedCluster(nodes, mtls, dataDir);
        }
        catch (InvalidOperationException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync();

            mtls?.Dispose();
            dataDir?.Dispose();
            throw;
        }
        catch (IOException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync();

            mtls?.Dispose();
            dataDir?.Dispose();
            throw;
        }
    }
}
