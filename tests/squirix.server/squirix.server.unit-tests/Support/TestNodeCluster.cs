using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Starts server unit-test clusters from the reserved unit-test port pool.</summary>
internal static class TestNodeCluster
{
    /// <summary>Starts a single node, reserving a port and creating the cluster owned by the caller.</summary>
    /// <param name="name">Label used for the temporary persistence directory when persistence is enabled.</param>
    /// <param name="nodeId">Node identifier to start.</param>
    /// <param name="replicaCount">Configured replica count for the node.</param>
    /// <param name="persistence">When <see langword="true" />, the node receives an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal static ValueTask<TestCluster<ClusterStartOptions>> StartAsync(
        string name,
        string nodeId,
        int replicaCount = 1,
        bool persistence = false,
        CancellationToken cancellationToken = default) => StartAsync(name, replicaCount, persistence, cancellationToken, nodeId);

    /// <summary>Starts three nodes, reserving ports and sharing mTLS material across the topology.</summary>
    /// <param name="name">Label used for the temporary persistence directory when persistence is enabled.</param>
    /// <param name="nodeA">First node identifier.</param>
    /// <param name="nodeB">Second node identifier.</param>
    /// <param name="nodeC">Third node identifier.</param>
    /// <param name="replicaCount">Configured replica count for every node.</param>
    /// <param name="persistence">When <see langword="true" />, each node receives an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    internal static ValueTask<TestCluster<ClusterStartOptions>> StartAsync(
        string name,
        string nodeA,
        string nodeB,
        string nodeC,
        int replicaCount = 1,
        bool persistence = false,
        CancellationToken cancellationToken = default) => StartAsync(name, replicaCount, persistence, cancellationToken, nodeA, nodeB, nodeC);

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Ownership of the identity and data directory transfers to the returned cluster, which disposes them.")]
    private static ValueTask<TestCluster<ClusterStartOptions>> StartAsync(
        string name,
        int replicaCount,
        bool persistence,
        CancellationToken cancellationToken,
        params ReadOnlySpan<string> nodeIds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        for (var i = 0; i < nodeIds.Length; i++)
            ArgumentException.ThrowIfNullOrWhiteSpace(nodeIds[i]);

        var held = ListenPortPool.ServerUnitTests.HoldPorts(nodeIds.Length);
        var identity = nodeIds.Length > 1 ? new ClusterIdentity() : null;
        var topology = new ClusterNode[nodeIds.Length];
        for (var i = 0; i < nodeIds.Length; i++)
            topology[i] = new ClusterNode(nodeIds[i], held[i].HttpUri);

        return StartHeldAsync(replicaCount, identity, persistence ? new TempDirectory(name) : null, held, topology, cancellationToken);
    }

    private static async ValueTask<TestCluster<ClusterStartOptions>> StartHeldAsync(
        int replicaCount,
        ClusterIdentity? identity,
        TempDirectory? dataDir,
        HeldPort[] held,
        ClusterNode[] topology,
        CancellationToken cancellationToken)
    {
        var cluster = TestCluster<ClusterStartOptions>.Create(topology, identity: identity, dataDir: dataDir);

        try
        {
            for (var i = 0; i < topology.Length; i++)
            {
                var options = new ClusterStartOptions
                {
                    ReplicaCount = replicaCount,
                    DataDir = dataDir == null ? null : NodePathKit.Combine(dataDir.Path, topology[i].NodeId),
                };
                _ = await cluster.StartNodeAsync(topology[i].NodeId, options, cancellationToken).ConfigureAwait(false);
            }

            return cluster;
        }
        catch
        {
            var started = cluster.StartedCount;
            await cluster.DisposeAsync().ConfigureAwait(false);

            for (var i = started; i < held.Length; i++)
                held[i].Dispose();

            throw;
        }
    }
}
