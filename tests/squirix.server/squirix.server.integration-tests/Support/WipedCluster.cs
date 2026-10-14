using System;
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
    /// <returns>The options.</returns>
    internal static IntegrationStartOptions Options(string scope, bool failover, PersistenceOptions? persistence = null) => new()
    {
        PersistenceOptions = persistence,
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = true,
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
}
