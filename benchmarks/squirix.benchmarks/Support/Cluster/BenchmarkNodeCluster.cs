using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Benchmarks.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Starts the single in-process node used by client SDK benchmarks.</summary>
internal static class BenchmarkNodeCluster
{
    /// <summary>Starts a benchmark node, reserving a loopback port and warming up one client connection.</summary>
    /// <param name="durabilityMode">Ephemeral or persistent node durability.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal static Task<TestCluster<ClusterStartOptions>> StartAsync(
        BenchmarkDurabilityMode durabilityMode = BenchmarkDurabilityMode.Ephemeral,
        CancellationToken cancellationToken = default)
    {
        var uri = ListenPortPool.ServerBenchmarks.HoldHttpUri();
        return StartAsync($"bench-{Guid.NewGuid():N}", uri, durabilityMode, cancellationToken);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Ownership of the data directory transfers to the cluster, which disposes it.")]
    private static async Task<TestCluster<ClusterStartOptions>> StartAsync(string nodeId, Uri uri, BenchmarkDurabilityMode durabilityMode, CancellationToken cancellationToken)
    {
        var dir = durabilityMode is BenchmarkDurabilityMode.Persistence ? new TempDirectory("squirix-bench") : null;
        var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode(nodeId, uri), dataDir: dir);

        try
        {
            var options = dir == null ? null : new ClusterStartOptions { DataDir = dir };
            _ = await cluster.StartNodeAsync(nodeId, options, cancellationToken).ConfigureAwait(false);

            // Warm up one client connection so the first measured operation does not pay connect cost.
            var unused = await BenchmarkClientLease.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);
            await unused.DisposeAsync().ConfigureAwait(false);
            return cluster;
        }
        catch (Exception ex) when (ex is InvalidOperationException or IOException)
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
