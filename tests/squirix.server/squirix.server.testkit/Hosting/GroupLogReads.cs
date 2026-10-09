using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads the group log of one node of a test cluster.</summary>
internal static class GroupLogReads
{
    /// <summary>Reads the index of the last entry a running node holds in the log of a group, committed or not.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <param name="nodeId">The node, which must run.</param>
    /// <param name="groupId">The replica group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The last log index; zero for an empty log.</returns>
    /// <exception cref="InvalidOperationException">The node does not hold the log of the group open.</exception>
    internal static async ValueTask<ulong> LastIndexAsync<TOptions>(TestCluster<TOptions> cluster, string nodeId, string groupId, CancellationToken cancellationToken)
        where TOptions : ClusterStartOptions
    {
        ArgumentNullException.ThrowIfNull(cluster);
        var registry = cluster[nodeId].GetRequiredService<ReplicaGroupRegistry>();
        var log = registry.TryGetLog(groupId, out var open) ? open : throw new InvalidOperationException($"The group log {groupId} is not open on node {nodeId}.");
        return (await log.GetStatusAsync(cancellationToken).ConfigureAwait(false)).LastLogIndex;
    }
}
