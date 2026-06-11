using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.Server.TestKit.AspNetCore;
using DirectoryKit = Squirix.E2EBenchmarks.Utils.DirectoryKit;
using PathKit = Squirix.E2EBenchmarks.Utils.PathKit;

namespace Squirix.E2EBenchmarks.Infrastructure;

/// <summary>
/// Owns real Squirix nodes for an end-to-end benchmark scenario.
/// </summary>
internal sealed class E2EBenchmarkCluster : IAsyncDisposable
{
    private readonly Dictionary<string, TestNodeHost> _nodes;
    private readonly string _rootDataDir;
    private BenchmarkClientLease? _client;
    private int _disposed;

    private E2EBenchmarkCluster(Dictionary<string, TestNodeHost> nodes, string rootDataDir)
    {
        _nodes = nodes;
        _rootDataDir = rootDataDir;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        try
        {
            if (_client is not null)
                await _client.DisposeAsync().ConfigureAwait(false);

            foreach (var node in _nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            if (!string.IsNullOrWhiteSpace(_rootDataDir))
                DirectoryKit.TryDeleteDirectory(_rootDataDir);
        }
    }

    internal static async Task<E2EBenchmarkCluster> StartAsync(BenchmarkTopology topology, E2EBenchmarkDurabilityMode durabilityMode, CancellationToken cancellationToken)
    {
        BenchmarkRuntime.EnsureInitialized();
        var nodeIds = topology == BenchmarkTopology.SingleNode ? new[] { "nodeA" } : ["nodeA", "nodeB"];
        var addresses = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var nodeId in nodeIds)
            addresses[nodeId] = E2EBenchmarkPortAllocator.NextHttpUrl();

        var peers = new (string NodeId, string Address)[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            peers[i] = (nodeIds[i], addresses[nodeIds[i]]);

        var usePersistence = durabilityMode == E2EBenchmarkDurabilityMode.Persistence;
        var root = usePersistence
            ? PathKit.Combine(Path.GetTempPath(), "squirix-e2e-benchmarks", $"{Environment.ProcessId:D}", Guid.NewGuid().ToString("N"))
            : string.Empty;
        if (usePersistence)
            _ = Directory.CreateDirectory(root);

        var nodes = new Dictionary<string, TestNodeHost>(StringComparer.Ordinal);

        try
        {
            foreach (var nodeId in nodeIds)
            {
                nodes[nodeId] = usePersistence
                    ? await TestNodeHostFactory.StartNodeAsync(nodeId, addresses[nodeId], peers, PathKit.Combine(root, nodeId), cancellationToken).ConfigureAwait(false)
                    : await TestNodeHostFactory.StartNodeAsync(nodeId, addresses[nodeId], peers, cancellationToken).ConfigureAwait(false);
            }

            return new E2EBenchmarkCluster(nodes, root);
        }
        catch (InvalidOperationException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);
            DirectoryKit.TryDeleteDirectory(root);
            throw;
        }
        catch (IOException)
        {
            foreach (var node in nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);
            DirectoryKit.TryDeleteDirectory(root);
            throw;
        }
    }

    internal async Task<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= await BenchmarkClientLease.ConnectAsync(_nodes["nodeA"].Address, cancellationToken).ConfigureAwait(false);
        return await _client.Client.GetCacheAsync<T>(cacheName, cancellationToken).ConfigureAwait(false);
    }
}
