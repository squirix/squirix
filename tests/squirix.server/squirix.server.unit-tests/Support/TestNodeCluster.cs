using System;
using System.Runtime.CompilerServices;
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
    /// <param name="nodeId">Node identifier to start.</param>
    /// <param name="replicaCount">Configured replica count for the node.</param>
    /// <param name="persistence">When <see langword="true" />, the node receives an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="name">Label used for the temporary persistence directory when persistence is enabled.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal static ValueTask<TestCluster<ClusterStartOptions>> StartAsync(
        string nodeId,
        int replicaCount = 1,
        bool persistence = false,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? name = null) => StartCoreAsync(name, replicaCount, persistence, cancellationToken, nodeId);

    /// <summary>Starts three nodes, reserving ports and sharing mTLS material across the topology.</summary>
    /// <param name="nodeA">First node identifier.</param>
    /// <param name="nodeB">Second node identifier.</param>
    /// <param name="nodeC">Third node identifier.</param>
    /// <param name="replicaCount">Configured replica count for every node.</param>
    /// <param name="persistence">When <see langword="true" />, each node receives an isolated data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="name">Label used for the temporary persistence directory when persistence is enabled.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    internal static ValueTask<TestCluster<ClusterStartOptions>> StartAsync(
        string nodeA,
        string nodeB,
        string nodeC,
        int replicaCount = 1,
        bool persistence = false,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? name = null) => StartCoreAsync(name, replicaCount, persistence, cancellationToken, nodeA, nodeB, nodeC);

    private static ValueTask<TestCluster<ClusterStartOptions>> StartCoreAsync(
        string? name,
        int replicaCount,
        bool persistence,
        CancellationToken cancellationToken,
        params ReadOnlySpan<string> nodeIds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        for (var i = 0; i < nodeIds.Length; i++)
            ArgumentException.ThrowIfNullOrWhiteSpace(nodeIds[i]);

        var held = ListenPortPool.ServerUnitTests.HoldPorts(nodeIds.Length);
        ClusterIdentity? identity = null;
        ClusterNode[] topology;
        ClusterIdentity? startIdentity;
        try
        {
            identity = nodeIds.Length > 1 ? new ClusterIdentity() : null;
            topology = new ClusterNode[nodeIds.Length];
            for (var i = 0; i < nodeIds.Length; i++)
                topology[i] = new ClusterNode(nodeIds[i], held[i].HttpUri);

            startIdentity = identity;
            identity = null;
        }
        finally
        {
            identity?.Dispose();
        }

        return StartHeldAsync(replicaCount, startIdentity, persistence ? name : null, held, topology, cancellationToken);
    }

    private static async ValueTask<TestCluster<ClusterStartOptions>> StartHeldAsync(
        int replicaCount,
        ClusterIdentity? identity,
        string? dataDirName,
        HeldPort[] held,
        ClusterNode[] topology,
        CancellationToken cancellationToken)
    {
        TestCluster<ClusterStartOptions>? cluster = null;
        try
        {
            cluster = TestCluster<ClusterStartOptions>.Create(topology, identity: identity, dir: dataDirName);
            var dataDirPath = cluster.DataDir?.ToString();
            var started = await cluster.StartAllAsync(
                nodeId => new ClusterStartOptions
                {
                    ReplicaCount = replicaCount,
                    DataDir = dataDirPath == null ? null : NodePathKit.Combine(dataDirPath, nodeId),
                },
                i => held[i].Dispose(),
                cancellationToken).ConfigureAwait(false);
            cluster = null;
            return started;
        }
        finally
        {
            if (cluster != null)
                await cluster.DisposeAsync().ConfigureAwait(false);
        }
    }
}
