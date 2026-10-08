using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// On a three-node cluster with automatic failover on, the group log audit passes for writes committed once, retried under the same
/// operation identifier, and continued under a new leader after the first one stops.
/// </summary>
public sealed class GroupLogAuditTests : NodeIntegrationTestBase
{
    private const string Group = "node-a";
    private const string Scope = "log-audit";

    /// <summary>Bounds every wait; the timing below elects within seconds, the rest absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly string[] Three = ["node-a", "node-b", "node-c"];

    /// <summary>Writes committed through the leader, one of them retried, appear once each and identically on every member.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedWritesPassTheAudit(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], Options("audit-writes"), cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (leader, _) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);
        var key = cluster[leader].FindKeyOwnedBy(Scope, Group);
        var operations = new string[5];
        for (var i = 0; i < operations.Length; i++)
        {
            operations[i] = Guid.NewGuid().ToString("N");
            await CommitAsync(probe, cluster[leader], key, operations[i], cancellationToken);
        }

        await CommitAsync(probe, cluster[leader], key, operations[0], cancellationToken);
        var report = await GroupLogAudit.RunAsync(cluster, Group, Three, Bound, cancellationToken);

        _ = await Assert.That(report.ClientEntries).IsEqualTo(operations.Length);
        _ = await Assert.That(report.SharedFrom).IsLessThanOrEqualTo(report.SharedTo);
    }

    /// <summary>
    /// Writes before and after the leader stops, and a retry of an earlier write through the new leader, appear once each and identically on
    /// the two remaining members.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AuditHoldsAcrossLeaderStop(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], Options("audit-failover"), cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);
        var key = cluster[former].FindKeyOwnedBy(Scope, Group);
        var before = Guid.NewGuid().ToString("N");
        await CommitAsync(probe, cluster[former], key, before, cancellationToken);

        await cluster.StopNodeAsync(former);
        var survivors = Array.FindAll(Three, id => !string.Equals(id, former, StringComparison.Ordinal));
        _ = await probe.WaitForNewLeaderAsync(Group, formerTerm, Bound, cancellationToken);
        var (next, _) = await probe.WaitForStableLeaderAsync(Group, survivors, Bound, cancellationToken);
        await CommitAsync(probe, cluster[next], key, Guid.NewGuid().ToString("N"), cancellationToken);
        await CommitAsync(probe, cluster[next], key, before, cancellationToken);
        var report = await GroupLogAudit.RunAsync(cluster, Group, survivors, Bound, cancellationToken);

        _ = await Assert.That(report.ClientEntries).IsEqualTo(2);
    }

    /// <summary>Commits one write through the committers of a node with authority, retrying while the group is not ready to take it.</summary>
    /// <param name="probe">The leader probe whose ledger checks election safety on every attempt.</param>
    /// <param name="node">The node with authority.</param>
    /// <param name="key">A key of the group.</param>
    /// <param name="operationId">The operation identifier every attempt carries, so a retry never writes twice.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the write committed.</returns>
    private static Task CommitAsync(ClusterLeaderProbe<IntegrationStartOptions> probe, ITestNodeHost node, string key, string operationId, CancellationToken cancellationToken) =>
        probe.Ledger(Group).UntilValueAsync(
            (Committers: node.GetRequiredService<ReplicaGroupCommitters>(), Key: key, OperationId: operationId),
            static async (write, token) =>
            {
                try
                {
                    await write.Committers.ForKey(Scope, write.Key).CommitSetAsync(write.OperationId, Scope, write.Key, new NodeCacheEntry<object?> { Value = write.OperationId }, token);
                    return true;
                }
                catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable or StatusCode.ResourceExhausted)
                {
                    return false;
                }
                catch (SquirixException)
                {
                    return false;
                }
            },
            "a write commits",
            cancellationToken);

    private static IntegrationStartOptions Options(string scope) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = true,
        ElectionTiming = new TestElectionTiming(),
    };
}
