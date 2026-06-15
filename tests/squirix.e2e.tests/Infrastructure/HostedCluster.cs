using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Lifecycle wrapper for a started Squirix test cluster (single- or multi-node).
/// </summary>
internal sealed class HostedCluster : IAsyncDisposable
{
    private readonly List<ISquirixClient> _clients = [];
    private readonly MtlsTestContext? _mtls;
    private readonly Dictionary<string, TestNode> _nodes;
    private int _disposed;

    private HostedCluster(Dictionary<string, TestNode> nodes, MtlsTestContext? mtls)
    {
        _nodes = nodes;
        _mtls = mtls;
    }

    public static ValueTask<HostedCluster> StartSingleNodeAsync(
        string? testName = null,
        TestNodeSecurityOptions? security = null,
        bool usePersistence = false,
        CancellationToken cancellationToken = default) => StartAsync(["nodeA"], new TwoNodeStartOptions { Security = security }, testName, usePersistence, cancellationToken);

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
        var url = _nodes[nodeId].Address;
        var client = await LoopbackConnect.ConnectAsync(url, cancellationToken);
        _clients.Add(client);
        return client;
    }

    public string GetAddress(string nodeId) => _nodes[nodeId].Address;

    /// <summary>
    /// Stops and removes one HostedCluster node while leaving other nodes running.
    /// </summary>
    /// <param name="nodeId">Node identifier to stop.</param>
    /// <returns>A task that completes when the node has been stopped.</returns>
    public async ValueTask StopNodeAsync(string nodeId)
    {
        if (!_nodes.Remove(nodeId, out var node))
            throw new InvalidOperationException($"Node '{nodeId}' is not running.");

        await node.DisposeAsync();
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
    }

    private static string BuildDataDir(string nodeId, string? testName)
    {
        var scope = string.IsNullOrWhiteSpace(testName) ? "unknown" : testName;
        var root = PathKit.Combine(Path.GetTempPath(), "squirix-e2e");
        var target = PathKit.Combine(root, $"{scope}__{Environment.ProcessId}", nodeId, Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(target);
        return target;
    }

    private static async ValueTask<HostedCluster> StartAsync(
        string[] nodeIds,
        TwoNodeStartOptions? startOptions,
        string? testName,
        bool usePersistence,
        CancellationToken cancellationToken = default)
    {
        startOptions ??= new TwoNodeStartOptions();
        var urls = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < nodeIds.Length; i++)
            urls[nodeIds[i]] = ListenPortPool.EndToEndTests.NextHttpAddress();

        var topology = new (string NodeId, string Address)[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            topology[i] = (nodeIds[i], urls[nodeIds[i]]);

        var nodes = new Dictionary<string, TestNode>(StringComparer.Ordinal);
        var mtls = nodeIds.Length > 1 ? new MtlsTestContext() : null;
        try
        {
            for (var i = 0; i < nodeIds.Length; i++)
            {
                var nodeId = nodeIds[i];
                var hostOptions = new TestNodeHostStartOptions
                {
                    DataDir = usePersistence ? BuildDataDir(nodeId, testName) : null,
                    Security = startOptions.Security,
                    Mtls = mtls,
                    MtlsProfile = startOptions.GetProfile(nodeId),
                };
                nodes[nodeId] = new TestNode(await TestNodeHostFactory.StartNodeAsync(nodeId, urls[nodeId], topology, hostOptions, cancellationToken));
            }

            return new HostedCluster(nodes, mtls);
        }
        catch (InvalidOperationException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync();

            mtls?.Dispose();
            throw;
        }
        catch (IOException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync();

            mtls?.Dispose();
            throw;
        }
    }
}
