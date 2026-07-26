using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2EBenchmarks.Support.Cluster;

/// <summary>Owns real Squirix nodes for an end-to-end benchmark scenario.</summary>
internal sealed class E2EBenchmarkCluster : IAsyncDisposable
{
    private static readonly string[] DualNodeIds = ["nodeA", "nodeB"];
    private static readonly string[] SingleNodeIds = ["nodeA"];

    private readonly TempDirectory? _dataDir;
    private readonly FrozenDictionary<string, TestNodeHost> _nodes;
    private E2EBenchmarkClientLease? _client;
    private int _disposed;

    private E2EBenchmarkCluster(FrozenDictionary<string, TestNodeHost> nodes, TempDirectory? dataDir)
    {
        _nodes = nodes;
        _dataDir = dataDir;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        if (_client is not null)
            await _client.DisposeAsync().ConfigureAwait(false);

        foreach (var node in _nodes.Values)
            await node.DisposeAsync().ConfigureAwait(false);

        _dataDir?.Dispose();
    }

    internal static async Task<E2EBenchmarkCluster> StartAsync(BenchmarkTopology topology, E2EBenchmarkDurabilityMode durabilityMode, CancellationToken cancellationToken)
    {
        var nodeIds = topology is BenchmarkTopology.SingleNode ? SingleNodeIds : DualNodeIds;
        var addresses = new Dictionary<string, Uri>(StringComparer.Ordinal);

        // Allocate listener URIs up front so every node advertises the same peer topology during startup.
        foreach (var nodeId in nodeIds)
            addresses[nodeId] = ListenPortPool.EndToEndBenchmarks.NextHttpUri();

        var peers = new (string NodeId, Uri Uri)[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            peers[i] = (nodeIds[i], addresses[nodeIds[i]]);

        var usePersistence = durabilityMode is E2EBenchmarkDurabilityMode.Persistence;
        var dataDir = usePersistence ? new TempDirectory("squirix-e2e-benchmarks") : null;

        var nodes = new Dictionary<string, TestNodeHost>(StringComparer.Ordinal);

        try
        {
            // Each node receives an isolated data directory when persistence benchmarks are enabled.
            foreach (var nodeId in nodeIds)
            {
                nodes[nodeId] = usePersistence
                    ? await TestNodeHostFactory.StartNodeAsync(nodeId, addresses[nodeId], peers, Path.Join(dataDir!.Path, nodeId), cancellationToken).ConfigureAwait(false)
                    : await TestNodeHostFactory.StartNodeAsync(nodeId, addresses[nodeId], peers, cancellationToken).ConfigureAwait(false);
            }

            return new E2EBenchmarkCluster(nodes.ToFrozenDictionary(StringComparer.Ordinal), dataDir);
        }
        catch (InvalidOperationException)
        {
            // Partial startup must tear down already-started nodes and the temp data directory.
            foreach (var node in nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);
            dataDir?.Dispose();
            throw;
        }
        catch (IOException)
        {
            // IO failures during host spin-up use the same rollback path as configuration errors.
            foreach (var node in nodes.Values)
                await node.DisposeAsync().ConfigureAwait(false);
            dataDir?.Dispose();
            throw;
        }
    }

    internal async Task<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= await E2EBenchmarkClientLease.ConnectAsync(_nodes["nodeA"].Uri, cancellationToken).ConfigureAwait(false);
        return await _client.Client.GetCacheAsync<T>(cacheName, cancellationToken).ConfigureAwait(false);
    }
}
