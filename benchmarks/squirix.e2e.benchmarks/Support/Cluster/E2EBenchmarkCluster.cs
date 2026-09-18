using System;
using System.Diagnostics.CodeAnalysis;
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

    private readonly TestCluster<ClusterStartOptions> _cluster;
    private E2EBenchmarkClientLease? _client;
    private int _disposed;

    private E2EBenchmarkCluster(TestCluster<ClusterStartOptions> cluster)
    {
        _cluster = cluster;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        if (_client != null)
            await _client.DisposeAsync().ConfigureAwait(false);

        await _cluster.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Starts the nodes for the requested topology and durability mode.</summary>
    /// <param name="topology">Single- or dual-node topology.</param>
    /// <param name="durabilityMode">Ephemeral or persistent node durability.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started benchmark cluster owning the nodes.</returns>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership of the data directory transfers to the cluster, which disposes it.")]
    internal static async Task<E2EBenchmarkCluster> StartAsync(BenchmarkTopology topology, DurabilityMode durabilityMode, CancellationToken cancellationToken)
    {
        var nodeIds = topology is BenchmarkTopology.SingleNode ? SingleNodeIds : DualNodeIds;
        var usePersistence = durabilityMode is DurabilityMode.Persistence;
        var reserved = Array.Empty<HeldPort>();
        TempDirectory? dataDir = null;
        TestCluster<ClusterStartOptions>? cluster = null;
        try
        {
            // Allocate listener URIs up front so every node advertises the same peer topology during startup.
            // Handles stay alive until each node binds (released by the factory) or startup fails.
            reserved = ListenPortPool.EndToEndBenchmarks.HoldPorts(nodeIds.Length);
            var peers = new ClusterNode[nodeIds.Length];
            for (var i = 0; i < nodeIds.Length; i++)
                peers[i] = new ClusterNode(nodeIds[i], reserved[i].HttpUri);

            dataDir = usePersistence ? new TempDirectory("squirix-e2e-benchmarks") : null;
            cluster = TestCluster<ClusterStartOptions>.Create(peers, dataDir: dataDir);

            // Each node receives an isolated data directory when persistence benchmarks are enabled.
            for (var i = 0; i < nodeIds.Length; i++)
            {
                var options = usePersistence ? new ClusterStartOptions { DataDir = Path.Join(dataDir!.Path, nodeIds[i]) } : null;
                _ = await cluster.StartNodeAsync(nodeIds[i], options, cancellationToken).ConfigureAwait(false);
            }

            return new E2EBenchmarkCluster(cluster);
        }
        catch
        {
            // Roll back every setup failure, including cancellation: HeldPort.Dispose is
            // a no-op for ports the factory already released for binding.
            if (cluster == null)
                dataDir?.Dispose();
            else
                await cluster.DisposeAsync().ConfigureAwait(false);

            for (var i = 0; i < reserved.Length; i++)
                reserved[i].Dispose();

            throw;
        }
    }

    internal async Task<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= await E2EBenchmarkClientLease.ConnectAsync(_cluster["nodeA"].Uri, cancellationToken).ConfigureAwait(false);
        return await _client.Client.GetCacheAsync<T>(cacheName, cancellationToken).ConfigureAwait(false);
    }
}
