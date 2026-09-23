using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Benchmarks.Support.Client;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.Hosting;
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
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Best-effort rollback: the cluster disposal failure is logged and swallowed so the original start failure always propagates.")]
    private static async Task<TestCluster<ClusterStartOptions>> StartAsync(string nodeId, Uri uri, BenchmarkDurabilityMode mode, CancellationToken cancellationToken)
    {
        var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode(nodeId, uri), dir: mode is BenchmarkDurabilityMode.Persistence ? "squirix-bench" : null);

        try
        {
            _ = await cluster.StartNodeAsync(nodeId, new ClusterStartOptions { DataDir = cluster.DataDir?.ToString() }, cancellationToken).ConfigureAwait(false);
            var unused = await BenchmarkClientLease.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);
            await unused.DisposeAsync().ConfigureAwait(false);
            return cluster;
        }
        catch (Exception)
        {
            try
            {
                await cluster.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception disposeException)
            {
                TestLog.Suppressed("BenchmarkNodeCluster.StartAsync rollback: cluster disposal failed after a start failure.", disposeException);
            }

            throw;
        }
    }
}
