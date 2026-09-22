using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2EBenchmarks.Support.Cluster;

/// <summary>Starts the single in-process node used as the remote server for end-to-end benchmarks.</summary>
internal static class E2EBenchmarkNodeCluster
{
    /// <summary>Starts an end-to-end benchmark node, reserving a loopback port and warming up one client connection.</summary>
    /// <param name="durabilityMode">Ephemeral or persistent node durability.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal static Task<TestCluster<ClusterStartOptions>> StartAsync(DurabilityMode durabilityMode = DurabilityMode.Ephemeral, CancellationToken cancellationToken = default) =>
        StartAsync($"bench-{Guid.NewGuid():N}", ListenPortPool.EndToEndBenchmarks.HoldHttpUri(), durabilityMode, cancellationToken);

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Best-effort rollback: the cluster disposal failure is logged and swallowed so the original start failure always propagates.")]
    private static async Task<TestCluster<ClusterStartOptions>> StartAsync(string nodeId, Uri uri, DurabilityMode durabilityMode, CancellationToken cancellationToken)
    {
        var dataDirName = durabilityMode is DurabilityMode.Persistence ? "squirix-e2e-bench" : null;
        var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode(nodeId, uri), dir: dataDirName);

        try
        {
            var options = new ClusterStartOptions { DataDir = cluster.DataDir?.ToString() };
            _ = await cluster.StartNodeAsync(nodeId, options, cancellationToken).ConfigureAwait(false);

            // Warm up one client connection so the first measured operation does not pay connect cost.
            var unused = await E2EBenchmarkClientLease.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);
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
                TestLog.Suppressed("E2EBenchmarkNodeCluster.StartAsync rollback: cluster disposal failed after a start failure.", disposeException);
            }

            throw;
        }
    }
}
