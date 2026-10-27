using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A leader that a newer one deposed commits nothing from its old term: neither the entries the newer leader replaced in its log, nor an
/// entry whose late append reaches a follower that already moved to the newer term (#1158).
/// </summary>
public sealed class DeposedLeaderTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";

    /// <summary>The group node n1 leads by election, from slot 2; its followers are n2 and n3.</summary>
    private const string LedGroup = "n2";

    /// <summary>The group node n1 owns statically; its followers are n2 and n3.</summary>
    private const string OwnedGroup = "n1";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The leader of term 2 appends an entry no follower takes; a leader of term 3 replaces it in this node's log. The retirement then
    /// applies what the old term committed, and the replaced entry is neither committed nor applied.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetireAfterRewriteAppliesNothing(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "elected"), Groups, null, cancellationToken);
        var log = LogOf(registry, LedGroup);
        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var committer = CreateElectedCommitter(registry, LedGroup, gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, LedGroup, "n1"));
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        var replacement = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 3UL, Encoding.UTF8.GetBytes("newer")));
        var rewrite = await log.AppendAsync(new FollowerLogAppendRequest("n3", 3UL, 1UL, 2UL, 1UL, replacement), cancellationToken);
        var retired = await committer.RetireAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((rewrite.Success, retired)).IsEqualTo((true, true));
        _ = await Assert.That((status.CommitIndex, status.LastLogIndex, status.LastLogTerm)).IsEqualTo((1UL, 2UL, 3UL));
        _ = await Assert.That(cache.Applied).DoesNotContain("b");
    }

    /// <summary>
    /// The owner's append to n2 is held past the commit budget while n3 is down. Meanwhile a newer leader reaches n2, which refuses the
    /// late append of the old term: no majority forms, and the owner commits nothing.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateAppendAfterNewerTermCommitsNothing(CancellationToken cancellationToken)
    {
        var logs = new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal);
        try
        {
            foreach (var node in new[] { "n2", "n3" })
            {
                var follower = new FollowerLog(Path.Join(Dir, node), OwnedGroup, GroupComposition.Create(OwnedGroup), NullLogger<FollowerLog>.Instance, TimeProvider.System);
                logs[node] = follower;
                await follower.OpenSeededAsync(cancellationToken);
            }

            var gateway = new LateGateway(new FollowerLogRoutingGateway(logs));
            await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
            var log = LogOf(registry, OwnedGroup);
            var committer = CreateCommitter(registry, gateway, TimeSpan.FromMilliseconds(200));
            await using (committer.ConfigureAwait(false))
            {
                _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "k1", Entry("k1"), cancellationToken));
                await gateway.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
                var term = (await log.GetStatusAsync(cancellationToken)).CurrentTerm;
                _ = await logs["n2"].ObserveTermAsync(term + 1, cancellationToken);
                gateway.Release();
                var late = await gateway.Answered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
                _ = await Assert.That((late.Success, late.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.StaleTerm));
            }

            // The disposal awaited the observation of the late answer.
            var status = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((1UL, 0UL));
            _ = await Assert.That((await logs["n2"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(0UL);
        }
        finally
        {
            foreach (var (_, follower) in logs)
                await follower.DisposeAsync();
        }
    }

    private static IFollowerLog LogOf(ReplicaGroupRegistry registry, string groupId) =>
        registry.TryGetLog(groupId, out var log) ? log : throw new InvalidOperationException($"The group log {groupId} is not open.");

    /// <summary>Hands the election state of the led group the authority the driver grants once the leader-term entry is committed.</summary>
    /// <param name="registry">The registry serving the group.</param>
    /// <param name="term">The led term.</param>
    private static void GrantAuthority(ReplicaGroupRegistry registry, ulong term)
    {
        var state = registry.StateFor(LedGroup);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(term);
        _ = state.GrantAuthority(term);
    }

    /// <summary>Routes appends to the follower logs; fails every batch with entries to n3, and holds the first one to n2 until released.</summary>
    private sealed class LateGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource<FollowerLogAppendResult> _answered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IReplicaRpcGateway _routing;
        private int _armed = 1;

        internal LateGateway(IReplicaRpcGateway routing)
        {
            _routing = routing;
        }

        /// <summary>Gets a task that completes with n2's answer to the held batch.</summary>
        internal Task<FollowerLogAppendResult> Answered => _answered.Task;

        /// <summary>Gets a task that completes once the batch to n2 is held.</summary>
        internal Task Held => _held.Task;

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Entries.Count == 0)
                return await _routing.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);

            if (string.Equals(nodeId, "n3", StringComparison.Ordinal))
                throw new IOException("n3 is down.");

            if (Interlocked.Exchange(ref _armed, 0) != 1)
                return await _routing.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);

            // The held call ignores its cancellation, as a request already on the wire does.
            _ = _held.TrySetResult();
            await _release.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            var result = await _routing.AppendEntriesAsync(nodeId, header, batch, CancellationToken.None).ConfigureAwait(false);
            _ = _answered.TrySetResult(result);
            return result;
        }

        /// <summary>Lets the held batch reach n2.</summary>
        internal void Release() => _ = _release.TrySetResult();
    }
}
