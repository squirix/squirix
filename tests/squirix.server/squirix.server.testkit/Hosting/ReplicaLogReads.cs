using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads the group log of one node of a test cluster.</summary>
internal static class ReplicaLogReads
{
    /// <summary>Reads how far a node retains and has compacted its copy of a group log.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <param name="nodeId">The node, which must run.</param>
    /// <param name="groupId">The replica group.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The last log index of the copy and the index its published snapshot covers; zero when the log was never compacted.</returns>
    /// <exception cref="InvalidOperationException">The node does not serve the group.</exception>
    internal static async ValueTask<(ulong LastLogIndex, ulong SnapshotIndex)> ReadAsync<TOptions>(TestCluster<TOptions> cluster, string nodeId, string groupId, CancellationToken cancellationToken)
        where TOptions : ClusterStartOptions
    {
        ArgumentNullException.ThrowIfNull(cluster);
        if (!cluster[nodeId].GetRequiredService<ReplicaGroupRegistry>().TryGetLog(groupId, out var log))
            throw new InvalidOperationException($"Node {nodeId} does not serve group log {groupId}.");

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var retention = await log.GetRetentionAsync(cancellationToken).ConfigureAwait(false);
        return (status.LastLogIndex, retention.SnapshotIndex);
    }
}
