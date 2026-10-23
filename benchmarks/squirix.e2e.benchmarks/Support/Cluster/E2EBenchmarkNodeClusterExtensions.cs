using System.Threading;
using System.Threading.Tasks;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2EBenchmarks.Support.Cluster;

/// <summary>Single-node convenience over the end-to-end benchmark cluster.</summary>
internal static class E2EBenchmarkNodeClusterExtensions
{
    /// <param name="cluster">The benchmark cluster.</param>
    extension(TestCluster<ClusterStartOptions> cluster)
    {
        /// <summary>Connects a client lease against the single benchmark node.</summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The connected client lease.</returns>
        internal Task<E2EBenchmarkClientLease> OpenClientAsync(CancellationToken cancellationToken) =>
            E2EBenchmarkClientLease.ConnectAsync(cluster.Topology[0].Uri, cancellationToken);
    }
}
