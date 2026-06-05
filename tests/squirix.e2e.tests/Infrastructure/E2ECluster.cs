using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.AspNetCore;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Started cluster fixture for black-box SDK tests.
/// </summary>
internal sealed class E2ECluster : IAsyncDisposable
{
    private readonly List<E2EClientHandle> _clients = [];
    private readonly Dictionary<string, E2ENode> _nodes;
    private int _disposed;

    private E2ECluster(Dictionary<string, E2ENode> nodes)
    {
        _nodes = nodes;
    }

    public static ValueTask<E2ECluster> StartSingleNodeAsync(string? testName = null, CancellationToken cancellationToken = default) =>
        StartAsync(["nodeA"], testName, cancellationToken);

    public static ValueTask<E2ECluster> StartTwoNodeAsync(string? testName = null, CancellationToken cancellationToken = default) =>
        StartAsync(["nodeA", "nodeB"], testName, cancellationToken);

    public async ValueTask<E2EClientHandle> ConnectClientAsync(string nodeId = "nodeA", CancellationToken cancellationToken = default)
    {
        var url = _nodes[nodeId].Address;
        var client = await SquirixClient.ConnectAsync(url, cancellationToken).ConfigureAwait(false);
        var handle = new E2EClientHandle(client);
        _clients.Add(handle);
        return handle;
    }

    public string GetAddress(string nodeId) => _nodes[nodeId].Address;

    /// <summary>
    /// Stops and removes one cluster node while leaving other nodes running.
    /// </summary>
    /// <param name="nodeId">Node identifier to stop.</param>
    /// <returns>A task that completes when the node has been stopped.</returns>
    public async ValueTask StopNodeAsync(string nodeId)
    {
        if (!_nodes.Remove(nodeId, out var node))
            throw new InvalidOperationException($"Node '{nodeId}' is not running.");

        await node.DisposeAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        for (var i = _clients.Count - 1; i >= 0; i--)
            await _clients[i].DisposeAsync().ConfigureAwait(false);

        foreach (var node in _nodes.Values)
            await node.DisposeAsync().ConfigureAwait(false);
    }

    private static int AllocatePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static string BuildDataDir(string nodeId, string? testName)
    {
        var scope = string.IsNullOrWhiteSpace(testName) ? "unknown" : testName;
        var root = Path.Combine(Path.GetTempPath(), "squirix-e2e");
        var target = Path.Combine(root, $"{scope}__{Environment.ProcessId}", nodeId, Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(target);
        return target;
    }

    private static string GetNextHttpUrl() => $"http://127.0.0.1:{AllocatePort()}";

    private static async ValueTask<E2ECluster> StartAsync(string[] nodeIds, string? testName = null, CancellationToken cancellationToken = default)
    {
        var urls = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < nodeIds.Length; i++)
            urls[nodeIds[i]] = GetNextHttpUrl();

        var topology = new (string NodeId, string Address)[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            topology[i] = (nodeIds[i], urls[nodeIds[i]]);

        var nodes = new Dictionary<string, E2ENode>(StringComparer.Ordinal);
        try
        {
            for (var i = 0; i < nodeIds.Length; i++)
            {
                var nodeId = nodeIds[i];
                var host = await TestNodeHostFactory.StartNodeAsync(nodeId, urls[nodeId], topology, BuildDataDir(nodeId, testName), cancellationToken).ConfigureAwait(false);
                nodes[nodeId] = new E2ENode(host);
            }

            return new E2ECluster(nodes);
        }
        catch
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);

            throw;
        }
    }
}
