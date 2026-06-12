using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.TestKit.IO;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Started cluster fixture for black-box SDK tests.
/// </summary>
internal sealed class E2ECluster : IAsyncDisposable
{
    private readonly List<E2EClientHandle> _clients = [];
    private readonly MtlsTestContext? _mtls;
    private readonly Dictionary<string, E2ENode> _nodes;
    private int _disposed;

    private E2ECluster(Dictionary<string, E2ENode> nodes, MtlsTestContext? mtls)
    {
        _nodes = nodes;
        _mtls = mtls;
    }

    public static ValueTask<E2ECluster> StartSingleNodeAsync(
        string? testName = null,
        TestNodeSecurityOptions? security = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(["nodeA"], testName, security, usePersistence, cancellationToken);

    public static ValueTask<E2ECluster> StartTwoNodeAsync(
        string? testName = null,
        TestNodeSecurityOptions? security = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(["nodeA", "nodeB"], testName, security, usePersistence, cancellationToken);

    public async ValueTask<E2EClientHandle> ConnectClientAsync(string nodeId = "nodeA", CancellationToken cancellationToken = default)
    {
        var url = _nodes[nodeId].Address;
        var client = await E2ETestConnect.ConnectAsync(url, cancellationToken).ConfigureAwait(false);
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

        _mtls?.Dispose();
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
        var root = PathKit.Combine(Path.GetTempPath(), "squirix-e2e");
        var target = PathKit.Combine(root, $"{scope}__{Environment.ProcessId}", nodeId, Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(target);
        return target;
    }

    private static string GetNextHttpUrl() => $"https://127.0.0.1:{AllocatePort()}";

    private static async ValueTask<E2ECluster> StartAsync(
        string[] nodeIds,
        string? testName,
        TestNodeSecurityOptions? security,
        bool usePersistence,
        CancellationToken cancellationToken = default)
    {
        var urls = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < nodeIds.Length; i++)
            urls[nodeIds[i]] = GetNextHttpUrl();

        var topology = new (string NodeId, string Address)[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            topology[i] = (nodeIds[i], urls[nodeIds[i]]);

        var nodes = new Dictionary<string, E2ENode>(StringComparer.Ordinal);
        var mtls = nodeIds.Length > 1 ? new MtlsTestContext() : null;
        try
        {
            for (var i = 0; i < nodeIds.Length; i++)
            {
                var nodeId = nodeIds[i];
                var options = new TestNodeHostStartOptions
                {
                    DataDir = usePersistence ? BuildDataDir(nodeId, testName) : null,
                    Security = security,
                    Mtls = mtls,
                };
                nodes[nodeId] = new E2ENode(await TestNodeHostFactory.StartNodeAsync(nodeId, urls[nodeId], topology, options, cancellationToken).ConfigureAwait(false));
            }

            return new E2ECluster(nodes, mtls);
        }
        catch (InvalidOperationException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);

            mtls?.Dispose();
            throw;
        }
        catch (IOException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);

            mtls?.Dispose();
            throw;
        }
    }
}
