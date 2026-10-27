using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Pins how many durable boundaries each node of a healthy RF=3 and RF=2 group crosses per majority-committed write, so a change to the
/// number of metadata publishes shows up as one edited constant.
/// </summary>
public sealed class ReplicaPublishBudgetTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";
    private const int Writes = 20;

    /// <summary>Leader: the local append publishes nothing, as recovery derives the last log index from the log; one publish after the majority (commit index), per write.</summary>
    private const long LeaderMetaPerWrite = 1;

    /// <summary>Follower: one publish per accepted append, which also carries the leader commit index of the previous write; the clock never advances, so the commit index debounce never fires.</summary>
    private const long FollowerMetaPerWrite = 1;

    /// <summary>Every node appends one frame per write.</summary>
    private const long FramesPerWrite = 1;

    /// <summary>Every node flushes the appended frame once per write.</summary>
    private const long FlushesPerWrite = 1;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>N serial writes on a healthy RF=3 group publish metadata exactly the budgeted number of times on the leader and on each follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task SerialWritesStayWithinPublishBudget(CancellationToken cancellationToken) => AssertBudgetAsync(Dir, ["n2", "n3"], cancellationToken);

    /// <summary>N serial writes on a healthy two-copy group, whose majority is both copies, stay within the same per-node budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task TwoCopiesStayWithinPublishBudget(CancellationToken cancellationToken) => AssertBudgetAsync(Dir, ["n2"], cancellationToken);

    private static async Task AssertBudgetAsync(string dir, string[] followers, CancellationToken cancellationToken)
    {
        var leaderHooks = new CountingFollowerLogFaultHooks();
        var followerHooks = new ConcurrentDictionary<string, CountingFollowerLogFaultHooks>(StringComparer.Ordinal);
        var logs = new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal);
        try
        {
            foreach (var node in followers)
            {
                var hooks = new CountingFollowerLogFaultHooks();
                followerHooks[node] = hooks;
                var log = new FollowerLog(
                    Path.Join(dir, node),
                    GroupId,
                    GroupComposition.Create(GroupId),
                    NullLogger<FollowerLog>.Instance,
                    new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks });
                logs[node] = log;
                await log.OpenSeededAsync(cancellationToken);
            }

            var gateway = new FollowerLogRoutingGateway(logs);
            await using var registry = await OpenRegistryAsync(Path.Join(dir, "owner"), followers.Length + 1, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = leaderHooks }, cancellationToken);
            await using var committer = CreateCommitter(registry, gateway, followers);

            // The first write brings the coordinator up and settles the follower handshake; only later writes are measured.
            await committer.CommitSetAsync(NewOperationId(), "cache", "warm", Entry("warm"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await WaitForFollowersAsync(gateway, followers, 1, cancellationToken);

            var leaderBudget = new FollowerLogDurabilityCounts(LeaderMetaPerWrite, FramesPerWrite, FlushesPerWrite);
            var followerBudget = new FollowerLogDurabilityCounts(FollowerMetaPerWrite, FramesPerWrite, FlushesPerWrite);
            var leaderBefore = leaderHooks.Snapshot();
            var followerBefore = new FollowerLogDurabilityCounts[followers.Length];
            for (var f = 0; f < followers.Length; f++)
                followerBefore[f] = followerHooks[followers[f]].Snapshot();

            // Each write is checked on its own, so a write over the budget is not hidden by one under it.
            var index = 1UL;
            for (var i = 0; i < Writes; i++)
            {
                var key = $"k{i}";
                await committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
                index++;
                await WaitForFollowersAsync(gateway, followers, index, cancellationToken);

                var leaderAfter = leaderHooks.Snapshot();
                _ = await Assert.That(leaderAfter.Since(in leaderBefore)).IsEqualTo(leaderBudget).Because($"leader, write {i}");
                leaderBefore = leaderAfter;
                for (var f = 0; f < followers.Length; f++)
                {
                    var after = followerHooks[followers[f]].Snapshot();
                    _ = await Assert.That(after.Since(in followerBefore[f])).IsEqualTo(followerBudget).Because($"{followers[f]}, write {i}");
                    followerBefore[f] = after;
                }
            }
        }
        finally
        {
            foreach (var (_, log) in logs)
                await log.DisposeAsync();
        }
    }

    private static async Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, int replicaCount, FollowerLogOptions options, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, ["n1"], cancellationToken);
        var registry = new ReplicaGroupRegistry(dir, ["n1"], replicaCount, Fingerprint, 1, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, FollowerLogRoutingGateway gateway, string[] followers)
    {
        var cache = new StubCache();
        return new ReplicaGroupCommitter(
            registry,
            new OwnerLocator(followers),
            gateway,
            cache,
            ("n1", "n1"),
            new ReplicaTopologyStamp(Fingerprint, 1),
            NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Clock = TimeProvider.System,
            ShutdownTimeProvider = TimeProvider.System,
            BudgetTimeProvider = new FakeTimeProvider(),
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(cache, NullLogger<ReplicaGroupCommitter>.Instance, "n1", "n1"),
        };
    }

    private static async Task WaitForFollowersAsync(FollowerLogRoutingGateway gateway, string[] followers, ulong index, CancellationToken cancellationToken)
    {
        for (var i = 0; i < followers.Length; i++)
            await gateway.AppendedAsync(followers[i], index).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    private sealed class OwnerLocator : IReplicaGroupLocator
    {
        private readonly string[] _followers;

        internal OwnerLocator(string[] followers)
        {
            _followers = followers;
        }

        public int ReplicaCount => _followers.Length + 1;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            destination[0] = "n1";
            for (var i = 0; i < _followers.Length; i++)
                destination[i + 1] = _followers[i];
        }
    }
}
