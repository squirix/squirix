using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Steps the tests of copies on a lost disk share: the three-node cluster, the readiness of a node, writes through it, and its copy of a group.</summary>
internal static class WipedCluster
{
    /// <summary>The cache the tests write to.</summary>
    internal const string CacheName = "default";

    /// <summary>The owner of the group under test.</summary>
    internal const string OwnerId = "node-a";

    /// <summary>Bounds every wait for the cluster; the waits only absorb a loaded machine.</summary>
    internal static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    /// <summary>The nodes of the cluster; with three replicas each of them is a member of every group.</summary>
    internal static readonly string[] Nodes = [OwnerId, "node-b", "node-c"];

    private const string UnseededText = "is not seeded on this node";

    /// <summary>Gets the timing of the elections of the cluster: the pull request tier, with a fixed jitter.</summary>
    internal static TestElectionTiming Timing { get; } = new() { JitterSeed = 11UL };

    /// <summary>Gets the start options of the three-node cluster.</summary>
    /// <param name="scope">The scope of the data directories of the cluster.</param>
    /// <param name="failover">Whether automatic failover elects the leaders of the groups.</param>
    /// <param name="persistence">The persistence options of the node, with its data directory; a directory of the cluster when not set.</param>
    /// <param name="clean">Whether the data directory starts empty; a node started again keeps it.</param>
    /// <returns>The options.</returns>
    internal static IntegrationStartOptions Options(string scope, bool failover, PersistenceOptions? persistence = null, bool clean = true) => new()
    {
        PersistenceOptions = persistence,
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        AutomaticFailoverEnabled = failover,
        QuorumReadsEnabled = failover,
        ElectionTiming = Timing,
    };

    /// <summary>Gets the log of a group on a node.</summary>
    /// <param name="host">The node.</param>
    /// <param name="groupId">The group.</param>
    /// <returns>The log.</returns>
    /// <exception cref="InvalidOperationException">The node does not serve the group.</exception>
    internal static IFollowerLog Log(ITestNodeHost host, string groupId) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(groupId, out var log) ? log : throw new InvalidOperationException($"The node does not serve group {groupId}.");

    /// <summary>Waits until the owner of the group under test leads it with authority.</summary>
    /// <param name="cluster">The started cluster.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the owner leads.</returns>
    internal static Task AwaitOwnerLeadsAsync(TestCluster<IntegrationStartOptions> cluster, CancellationToken cancellationToken) =>
        cluster.WaitUntilAsync(static nodes => nodes[OwnerId].GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(OwnerId, out _), Bound, TimeProvider.System, cancellationToken);

    /// <summary>Waits until the replica group readiness check of every given node reports healthy.</summary>
    /// <param name="cluster">The started cluster.</param>
    /// <param name="nodeIds">The nodes.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once every node is ready.</returns>
    internal static Task AwaitReadyAsync(TestCluster<IntegrationStartOptions> cluster, string[] nodeIds, CancellationToken cancellationToken) =>
        (Cluster: cluster, NodeIds: nodeIds).WaitUntilValueAsync(
            static async (state, token) =>
            {
                foreach (var nodeId in state.NodeIds)
                {
                    if ((await state.Cluster[nodeId].ReplicaGroupsAsync(token)).Status != HealthStatus.Healthy)
                        return false;
                }

                return true;
            },
            Bound,
            TimeProvider.System,
            cancellationToken);

    /// <summary>Checks that a node reports not ready, with the verdict of a copy that is not seeded.</summary>
    /// <param name="host">The node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the check passed.</returns>
    internal static async Task AssertUnseededAsync(ITestNodeHost host, CancellationToken cancellationToken)
    {
        var (status, description) = await host.ReplicaGroupsAsync(cancellationToken);

        _ = await Assert.That(status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(description).Contains(UnseededText, StringComparison.Ordinal);
    }

    /// <summary>Writes a key of the group a node owns through the node and reads it back.</summary>
    /// <param name="host">The node, which owns the group and leads it.</param>
    /// <param name="ownerId">The owner of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the value was read back.</returns>
    internal static async Task WriteAndReadAsync(ITestNodeHost host, string ownerId, CancellationToken cancellationToken)
    {
        var cache = host.GetCache<object?>(CacheName);
        var key = host.FindKeyOwnedBy(CacheName, ownerId);

        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = "kept", Version = 1 }, cancellationToken);
        var read = await cache.GetValueAsync(CacheName, key, cancellationToken);

        _ = await Assert.That((read.Found, read.Value)).IsEqualTo((true, "kept"));
    }

    /// <summary>Checks that a node refuses a write to a key of the group it owns as unavailable.</summary>
    /// <param name="host">The node.</param>
    /// <param name="ownerId">The owner of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the check passed.</returns>
    internal static async Task AssertRefusesWriteAsync(ITestNodeHost host, string ownerId, CancellationToken cancellationToken)
    {
        var cache = host.GetCache<object?>(CacheName);
        var key = host.FindKeyOwnedBy(CacheName, ownerId);

        var failure = await NodeAsyncAssert.ThrowsAnyAsync<RpcException>(
            cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = "refused", Version = 1 }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }

    /// <summary>Checks that a node holds its copy of a group as an unseeded one at term zero, which has voted for nobody.</summary>
    /// <param name="host">The node.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the check passed.</returns>
    internal static async Task AssertUnseededCopyAsync(ITestNodeHost host, string groupId, CancellationToken cancellationToken)
    {
        var status = await Log(host, groupId).GetStatusAsync(cancellationToken);

        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.VotedFor)).IsEqualTo((GroupMemberState.Unseeded, 0UL, string.Empty));
    }

    /// <summary>Waits until a leader repaired the copy of a group on a node that lost its disk, and checks the vote it recorded.</summary>
    /// <param name="host">The node.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="leaderId">The leader that repairs the copy, which the copy holds its vote for.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The status of the repaired copy.</returns>
    /// <exception cref="TimeoutException">The copy was not repaired within <see cref="Bound" />.</exception>
    internal static async Task<FollowerLogStatus> AwaitRepairedCopyAsync(ITestNodeHost host, string groupId, string leaderId, CancellationToken cancellationToken)
    {
        var log = Log(host, groupId);
        try
        {
            await log.WaitUntilValueAsync(
                static async (copy, token) => (await copy.GetStatusAsync(token)).MemberState == GroupMemberState.Complete,
                Bound,
                TimeProvider.System,
                cancellationToken);
        }
        catch (TimeoutException exception)
        {
            var stuck = await log.GetStatusAsync(cancellationToken);
            throw new TimeoutException($"The copy of group {groupId} was not repaired within {Bound}; it is {stuck.MemberState} at term {stuck.CurrentTerm}.", exception);
        }

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.VotedFor).IsEqualTo(leaderId);
        return status;
    }

    /// <summary>
    /// Waits until the copy of a group on a node that lost its disk is complete and holds the term and the vote of the node that leads the group
    /// now, whichever node that is; an election that happened in the meantime only moves the leader and the term the copy is checked against.
    /// </summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="ledger">The ledger of the group, which checks election safety on every poll.</param>
    /// <param name="wipedId">The node that lost its disk; it is not taken for the leader that repairs its copy.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader of the group, the status of the repaired copy and the status of the leader's copy, taken together.</returns>
    /// <exception cref="TimeoutException">The copy did not take the vote of a leader within <see cref="Bound" />.</exception>
    internal static async Task<RepairedCopy> AwaitRepairedByLeaderAsync(
        TestCluster<IntegrationStartOptions> cluster,
        GroupAuthorityLedger<IntegrationStartOptions> ledger,
        string wipedId,
        string groupId,
        CancellationToken cancellationToken)
    {
        var others = new List<string>();
        foreach (var node in Nodes)
        {
            if (!string.Equals(node, wipedId, StringComparison.Ordinal))
                others.Add(node);
        }

        var observed = new RepairedCopy();
        var state = (Cluster: cluster, Ledger: ledger, Copy: Log(cluster[wipedId], groupId), Others: others, GroupId: groupId, Observed: observed);
        await ledger.UntilValueAsync(
            state,
            static async (wait, token) =>
            {
                var (leaderId, term) = wait.Ledger.Observe(wait.Others);
                if (term == 0UL)
                    return false;

                var copy = await wait.Copy.GetStatusAsync(token);
                if (copy.MemberState != GroupMemberState.Complete || copy.CurrentTerm != term || !string.Equals(copy.VotedFor, leaderId, StringComparison.Ordinal))
                    return false;

                var leader = await Log(wait.Cluster[leaderId], wait.GroupId).GetStatusAsync(token);
                if (leader.CurrentTerm != term)
                    return false;

                wait.Observed.Set(leaderId, in copy, in leader);
                return true;
            },
            $"the copy of group {groupId} is repaired and holds the vote of the leader of its term",
            cancellationToken);
        return observed;
    }

    /// <summary>Checks that a node holds its copy of a group as a complete one.</summary>
    /// <param name="host">The node.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the check passed.</returns>
    internal static async Task AssertCompleteCopyAsync(ITestNodeHost host, string groupId, CancellationToken cancellationToken)
    {
        var status = await Log(host, groupId).GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.MemberState).IsEqualTo(GroupMemberState.Complete);
    }

    /// <summary>The repaired copy of a group with the leader that repaired it, as one poll saw them together.</summary>
    internal sealed class RepairedCopy
    {
        /// <summary>Gets the leader of the group, which the copy holds its vote for.</summary>
        internal string LeaderId { get; private set; } = string.Empty;

        /// <summary>Gets the status of the repaired copy.</summary>
        internal FollowerLogStatus Repaired { get; private set; }

        /// <summary>Gets the status of the copy of the leader, in the term of the repaired copy.</summary>
        internal FollowerLogStatus Leader { get; private set; }

        /// <summary>Records what one poll saw.</summary>
        /// <param name="leaderId">The leader.</param>
        /// <param name="repaired">The status of the repaired copy.</param>
        /// <param name="leader">The status of the copy of the leader.</param>
        internal void Set(string leaderId, in FollowerLogStatus repaired, in FollowerLogStatus leader)
        {
            LeaderId = leaderId;
            Repaired = repaired;
            Leader = leader;
        }
    }
}
