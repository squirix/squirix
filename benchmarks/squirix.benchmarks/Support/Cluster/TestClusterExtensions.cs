using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Benchmarks.Support.Client;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Single-node convenience over the benchmark cluster.</summary>
internal static class TestClusterExtensions
{
    /// <param name="cluster">The benchmark cluster.</param>
    extension(TestCluster<ClusterStartOptions> cluster)
    {
        /// <summary>Gets the listen URI of the single benchmark node.</summary>
        /// <returns>The node listen URI.</returns>
        internal Uri Uri() => cluster.Topology[0].Uri;

        /// <summary>Gets the single benchmark node host.</summary>
        /// <returns>The node host.</returns>
        internal ITestNodeHost Host() => cluster[cluster.Topology[0].NodeId];

        /// <summary>Connects a client lease against the single benchmark node.</summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The connected client lease.</returns>
        internal Task<BenchmarkClientLease> OpenClientAsync(CancellationToken cancellationToken) => BenchmarkClientLease.ConnectAsync(cluster.Uri(), cancellationToken);
    }
}
