using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Binary;
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

    /// <summary>Concurrent inserts evict once on all three copies. A promoted follower chooses subsequent victims and a restarted member catches up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EvictionSurvivesLeaderChange(CancellationToken cancellationToken)
    {
        var options = Options("capacity-failover", true);
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], options, cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (former, term) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);
        var keys = OwnedKeys(cluster[former]);
        var oldLeader = cluster[former];
        await oldLeader.GetRequiredService<SnapshotTriggerService>().StopAsync(cancellationToken);
        await Task.WhenAll(CommitAsync(probe, oldLeader, keys[0], "first", cancellationToken), CommitAsync(probe, oldLeader, keys[1], "second", cancellationToken));
        await CommitAsync(probe, oldLeader, keys[2], "third", cancellationToken);
        await VerifyCopiesAsync(cluster, Three, former, keys, keys[2], cancellationToken);
        var snapshot = oldLeader.GetRequiredService<Coordinator>();
        _ = await Assert.That(await snapshot.SnapshotAsync(oldLeader.GetRequiredService<IJournalCoordinator>(), cancellationToken)).IsTrue();

        await cluster.StopNodeAsync(former);
        var survivors = Array.FindAll(Three, id => !string.Equals(id, former, StringComparison.Ordinal));
        _ = await probe.WaitForNewLeaderAsync(Group, term, Bound, cancellationToken);
        var (next, _) = await probe.WaitForStableLeaderAsync(Group, survivors, Bound, cancellationToken);
        await CommitAsync(probe, cluster[next], keys[3], "fourth", cancellationToken);
        await CommitAsync(probe, cluster[next], keys[2], "third", cancellationToken);
        _ = await cluster.StartNodeAsync(former, Options("capacity-failover", true, false), cancellationToken);
        await VerifyCopiesAsync(cluster, Three, next, keys, keys[3], cancellationToken);
    }

    /// <summary>A committed pressure removal is applied to all three replicas.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MemoryEvictionAgreesOnAllCopies(CancellationToken cancellationToken)
    {
        var options = Options("memory-eviction", budget: 2048);
        await using var cluster = await StartClusterAsync(Three[0], Three[1], Three[2], options, cancellationToken);
        var probe = new ClusterLeaderProbe<IntegrationStartOptions>(cluster);
        var (leader, _) = await probe.WaitForStableLeaderAsync(Group, Three, Bound, cancellationToken);
        var host = cluster[leader];
        var key = host.FindKeyOwnedBy(Scope, Group);
        var operation = Guid.NewGuid().ToString("N");
        await CommitAsync(probe, host, key, operation, cancellationToken, new string('x', 1800));
        var stats = host.GetRequiredService<ILocalCacheStats>();
        await probe.Ledger(Group).UntilValueAsync(stats, static (s, _) => ValueTask.FromResult(s.EntryCount == 0), "leader evicts under pressure", cancellationToken);
        var index = host.GetRequiredService<ReplicaGroupAppliers>().For(Group).AppliedIndex;
        foreach (var id in Three)
        {
            await cluster[id].GetRequiredService<ReplicaGroupAppliers>().For(Group).WaitAppliedAsync(index, cancellationToken).WaitAsync(Bound, TimeProvider.System, cancellationToken);
            _ = await Assert.That(cluster[id].GetRequiredService<ILocalCacheStats>().EntryCount).IsEqualTo(0);
        }

        var audit = await GroupLogAudit.RunAsync(cluster, Group, Three, Bound, cancellationToken);
        _ = await Assert.That(audit.ClientEntries).IsEqualTo(2);
    }

    private static string[] OwnedKeys(ITestNodeHost host)
    {
        var owners = host.GetRequiredService<INodeLocator>();
        var keys = new string[4];
        var found = 0;
        var i = 0;
        while (found < keys.Length)
        {
            var key = i.ToString(CultureInfo.InvariantCulture);
            i++;
            if (string.Equals(owners.GetOwner(Scope, key), Group, StringComparison.Ordinal))
                keys[found++] = key;
        }

        return keys;
    }

    private static async Task VerifyCopiesAsync(TestCluster<IntegrationStartOptions> cluster, string[] nodes, string leader, string[] keys, string newest, CancellationToken token)
    {
        var index = cluster[leader].GetRequiredService<ReplicaGroupAppliers>().For(Group).AppliedIndex;
        var expected = cluster[leader].GetRequiredService<PhysicalCache<StoredValue>>().RawReader;
        foreach (var id in nodes)
        {
            var host = cluster[id];
            await host.GetRequiredService<ReplicaGroupAppliers>().For(Group).WaitAppliedAsync(index, token).WaitAsync(Bound, TimeProvider.System, token);
            var physical = host.GetRequiredService<PhysicalCache<StoredValue>>();
            _ = await Assert.That(host.GetRequiredService<ILocalCacheStats>().EntryCount).IsEqualTo(2);
            _ = await Assert.That(physical.RawReader.GetEntryRaw(new CacheKey(Scope, newest), token)).IsNotNull();
            foreach (var key in keys)
            {
                var target = new CacheKey(Scope, key);
                _ = await Assert.That(StoredValueProbe.Text(physical.RawReader.GetEntryRaw(target, token)?.Value)).IsEqualTo(StoredValueProbe.Text(expected.GetEntryRaw(target, token)?.Value));
            }
        }
    }

    /// <summary>Commits one write through the committers of a node with authority, retrying while the group is not ready to take it.</summary>
    /// <param name="probe">The leader probe whose ledger checks election safety on every attempt.</param>
    /// <param name="node">The node with authority.</param>
    /// <param name="key">A key of the group.</param>
    /// <param name="operationId">The operation identifier every attempt carries, so a retry never writes twice.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="value">Optional payload for pressure tests.</param>
    /// <returns>A task that completes once the write committed.</returns>
    private static Task CommitAsync(ClusterLeaderProbe<IntegrationStartOptions> probe, ITestNodeHost node, string key, string operationId, CancellationToken cancellationToken, string? value = null) =>
        probe.Ledger(Group).UntilValueAsync(
            (Committers: node.GetRequiredService<ReplicaGroupCommitters>(), Key: key, OperationId: operationId, Value: value ?? operationId),
            static async (write, token) =>
            {
                try
                {
                    await write.Committers.ForKey(Scope, write.Key).CommitSetAsync(write.OperationId, Scope, write.Key, new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText(write.Value) }, token);
                    return true;
                }
                catch (ServerCallException exception) when (exception.StatusCode is FrameStatus.Unavailable or FrameStatus.ResourceExhausted)
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

    private static void ConfigureCapacity(TestServiceCollection services)
    {
        services.UseLruCapacity(2);
        _ = services.RemoveAll<TriggerOptions>().AddSingleton(new TriggerOptions { SnapshotEveryNOps = 1, MinGapBetweenSnapshots = TimeSpan.Zero });
    }

    private static IntegrationStartOptions Options(string scope, bool bounded = false, bool clean = true, long? budget = null) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        ExtraScope = scope,
        CleanTestDir = clean,
        EvictionBudgetBytes = budget,
        ServicesConfigure = bounded ? ConfigureCapacity : null,
        AutomaticFailoverEnabled = true,
        QuorumReadsEnabled = true,
        ElectionTiming = new TestElectionTiming(),
    };
}
