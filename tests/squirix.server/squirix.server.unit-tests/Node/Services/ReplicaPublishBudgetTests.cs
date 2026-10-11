using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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
/// Pins how many durable boundaries each node of a healthy RF=3 group crosses per majority-committed write, so a change to the
/// number of metadata publishes shows up as one edited constant.
/// </summary>
public sealed class ReplicaPublishBudgetTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";
    private const int Writes = 20;

    /// <summary>Leader: one publish after the local append (last log index) and one after the majority (commit index), per write.</summary>
    private const long LeaderMetaPerWrite = 2;

    /// <summary>Follower: one publish per accepted append, which also carries the leader commit index of the previous write.</summary>
    private const long FollowerMetaPerWrite = 1;

    /// <summary>Every node appends one frame per write.</summary>
    private const long FramesPerWrite = 1;

    /// <summary>Every node flushes the appended frame once per write.</summary>
    private const long FlushesPerWrite = 1;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>N serial writes on a healthy group publish metadata exactly the budgeted number of times on the leader and on each follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SerialWritesStayWithinPublishBudget(CancellationToken cancellationToken)
    {
        var leaderHooks = new CountingFollowerLogFaultHooks();
        var followerHooks = new ConcurrentDictionary<string, CountingFollowerLogFaultHooks>(StringComparer.Ordinal);
        var logs = new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal);
        try
        {
            foreach (var node in new[] { "n2", "n3" })
            {
                var hooks = new CountingFollowerLogFaultHooks();
                followerHooks[node] = hooks;
                var log = new FollowerLog(
                    Path.Join(Dir, node),
                    GroupId,
                    GroupComposition.Create(GroupId),
                    NullLogger<FollowerLog>.Instance,
                    new FollowerLogOptions { FaultHooks = hooks });
                logs[node] = log;
                await log.OpenAsync(cancellationToken);
            }

            var gateway = new FollowerLogRoutingGateway(logs);
            await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), new FollowerLogOptions { FaultHooks = leaderHooks }, cancellationToken);
            await using var committer = CreateCommitter(registry, gateway);

            // The first write brings the coordinator up and settles the follower handshake; only later writes are measured.
            await committer.CommitSetAsync(NewOperationId(), "cache", "warm", Entry("warm"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await WaitForFollowersAsync(gateway, 1, cancellationToken);

            var leaderBudget = new FollowerLogDurabilityCounts(LeaderMetaPerWrite, FramesPerWrite, FlushesPerWrite);
            var followerBudget = new FollowerLogDurabilityCounts(FollowerMetaPerWrite, FramesPerWrite, FlushesPerWrite);
            var leaderBefore = leaderHooks.Snapshot();
            var n2Before = followerHooks["n2"].Snapshot();
            var n3Before = followerHooks["n3"].Snapshot();

            // Each write is checked on its own, so a write over the budget is not hidden by one under it.
            var index = 1UL;
            for (var i = 0; i < Writes; i++)
            {
                var key = $"k{i}";
                await committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
                index++;
                await WaitForFollowersAsync(gateway, index, cancellationToken);

                var leaderAfter = leaderHooks.Snapshot();
                var n2After = followerHooks["n2"].Snapshot();
                var n3After = followerHooks["n3"].Snapshot();
                _ = await Assert.That(leaderAfter.Since(in leaderBefore)).IsEqualTo(leaderBudget).Because($"leader, write {i}");
                _ = await Assert.That(n2After.Since(in n2Before)).IsEqualTo(followerBudget).Because($"n2, write {i}");
                _ = await Assert.That(n3After.Since(in n3Before)).IsEqualTo(followerBudget).Because($"n3, write {i}");
                leaderBefore = leaderAfter;
                n2Before = n2After;
                n3Before = n3After;
            }
        }
        finally
        {
            foreach (var (_, log) in logs)
                await log.DisposeAsync();
        }
    }

    private static async Task WaitForFollowersAsync(FollowerLogRoutingGateway gateway, ulong index, CancellationToken cancellationToken)
    {
        await gateway.AppendedAsync("n2", index).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await gateway.AppendedAsync("n3", index).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }
}
