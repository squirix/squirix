using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Both ends real: the leader of a statically led group repairs a follower behind its compacted log, and the follower ends with the cache of the leader.</summary>
public sealed class ReplicaTransferEndToEndTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string GroupId = TransferFixtures.GroupId;
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The follower holds two entries the leader dropped from its log: the pass transfers the state, replays the entries above it, admits the
    /// follower, and a write the leader commits while the entries are streamed reaches the follower through the replay.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task FollowerEndsWithTheCacheOfTheLeader(CancellationToken cancellationToken) => RepairFollowerAsync(false, cancellationToken);

    /// <summary>
    /// The follower holds a complete copy with an empty log, so the position before the first entry the leader dropped is the origin: the session ends
    /// as compacted, not as corrupt, and the transfer gives the follower the cache of the leader.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task EmptyFollowerEndsWithTheCacheOfTheLeader(CancellationToken cancellationToken) => RepairFollowerAsync(true, cancellationToken);

    /// <summary>
    /// A key whose expiry the leader committed before the transfer is not brought back by it, and a key whose expiry the leader commits after the
    /// transferred index is removed by the replayed record, so neither is on the follower once it is admitted.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredKeysStayGone(CancellationToken cancellationToken)
    {
        using var followerDir = new TempDirectory("squirix-e2e-follower");
        using var leaderDir = new TempDirectory("squirix-e2e-leader");
        await using var harness = await ReplicaStateHarness.OpenAsync(followerDir, cancellationToken);
        await using var rig = new Rig(harness);
        await rig.OpenAsync(leaderDir, cancellationToken);
        var past = DateTime.UtcNow.AddHours(-1);
        await rig.CommitAsync("alive", null, cancellationToken);
        await rig.CommitAsync("dead", past, cancellationToken);
        await rig.CommitAsync("late", past, cancellationToken);
        _ = await rig.Committer.CommitExpiriesAsync([(CacheName, "dead")]);

        var pass = rig.StartPassAsync(cancellationToken);
        await rig.Gateway.EntriesHeld.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await rig.Committer.CommitExpiriesAsync([(CacheName, "late")]);
        rig.Gateway.ReleaseEntries();
        var admitted = await pass.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        await CatchUpAppliedAsync(harness, cancellationToken);
        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(await rig.Cache.GetEntryAsync(new CacheKey(CacheName, "dead"), cancellationToken)).IsNull();
        _ = await Assert.That(await rig.Cache.GetEntryAsync(new CacheKey(CacheName, "late"), cancellationToken)).IsNull();
        _ = await Assert.That(await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "dead"), cancellationToken)).IsNull();
        _ = await Assert.That(await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "late"), cancellationToken)).IsNull();
        _ = await Assert.That((await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "alive"), cancellationToken))?.Value).IsEqualTo("alive");
    }

    private static async Task RepairFollowerAsync(bool emptyLog, CancellationToken cancellationToken)
    {
        using var followerDir = new TempDirectory("squirix-e2e-follower");
        using var leaderDir = new TempDirectory("squirix-e2e-leader");
        if (emptyLog)
            await SeededLogs.SeedGroupsAsync(followerDir, [GroupId], cancellationToken);

        await using var harness = await ReplicaStateHarness.OpenAsync(followerDir, cancellationToken);
        await harness.SeedAsync(cancellationToken, "stale-1");
        await using var rig = new Rig(harness);
        await rig.OpenAsync(leaderDir, cancellationToken);

        var pass = rig.StartPassAsync(cancellationToken);
        await rig.Gateway.EntriesHeld.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await rig.CommitAsync("k5", null, cancellationToken);
        rig.Gateway.ReleaseEntries();
        var admitted = await pass.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = harness.Registry.TryGetLog(GroupId, out var followerLog);
        await CatchUpAppliedAsync(harness, cancellationToken);
        _ = await Assert.That(rig.Compacted).IsEqualTo(GroupCompactionOutcome.Compacted);
        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(rig.Registry.EligibilityFor(GroupId).StateFor(1)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(followerLog!.IsComplete).IsTrue();
        _ = await Assert.That(await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "stale-1"), cancellationToken)).IsNull();
        for (var i = 1; i <= 5; i++)
        {
            var leaderEntry = await rig.Cache.GetEntryAsync(new CacheKey(CacheName, $"k{i}"), cancellationToken);
            var followerEntry = await harness.Cache.GetEntryAsync(new CacheKey(CacheName, $"k{i}"), cancellationToken);
            _ = await Assert.That((followerEntry?.Value, followerEntry?.Version)).IsEqualTo((leaderEntry?.Value, leaderEntry?.Version));
            _ = await Assert.That(followerEntry).IsNotNull();
        }
    }

    private static async Task CatchUpAppliedAsync(ReplicaStateHarness harness, CancellationToken cancellationToken)
    {
        _ = harness.Registry.TryGetLog(GroupId, out var followerLog);
        await FollowerLogTestKit.MakeCommitDurableAsync(followerLog!, cancellationToken);
        var status = await followerLog!.GetStatusAsync(cancellationToken);
        await harness.Appliers.For(GroupId).CatchUpAsync(followerLog, status.LastAppliedIndex, status.CommitIndex, cancellationToken);
    }

    /// <summary>The leader n2 of a statically led group over a real log and cache, whose log was compacted past the follower n1 and which holds four committed keys.</summary>
    private sealed class Rig : IAsyncDisposable
    {
        private ReplicaGroupCommitter? _committer;
        private ReplicaGroupRegistry? _registry;
        private ReplicaStateTransfers? _transfers;

        internal Rig(ReplicaStateHarness harness)
        {
            Cache = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
            Gateway = new RoutedGateway(harness) { FollowerDown = true };
        }

        internal PhysicalCache<object?> Cache { get; }

        internal GroupCompactionOutcome Compacted { get; private set; }

        internal ReplicaGroupCommitter Committer => _committer ?? ThrowNotOpen<ReplicaGroupCommitter>();

        internal RoutedGateway Gateway { get; }

        internal ReplicaGroupRegistry Registry => _registry ?? ThrowNotOpen<ReplicaGroupRegistry>();

        public async ValueTask DisposeAsync()
        {
            if (_committer != null)
                await _committer.DisposeAsync();

            if (_registry != null)
                await _registry.DisposeAsync();
        }

        /// <summary>Opens the leader, commits k1 to k3 with the follower down, compacts the log through them, commits k4, and brings the follower up with its first entries message held.</summary>
        /// <param name="leaderDir">The data directory of the leader.</param>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>An asynchronous operation.</returns>
        internal async Task OpenAsync(string leaderDir, CancellationToken cancellationToken)
        {
            await SeededLogs.SeedGroupsAsync(leaderDir, [GroupId], cancellationToken);
            var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
            _registry = new ReplicaGroupRegistry(leaderDir, [GroupId], 3, TransferFixtures.Fingerprint, 1UL, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System };
            await _registry.OpenAsync(cancellationToken);
            var local = new ClientCache<object?>(Cache, Cache);
            _committer = new ReplicaGroupCommitter(_registry, new Members(), Gateway, local, (GroupId, "n2"), new ReplicaTopologyStamp(TransferFixtures.Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
            {
                Clock = TimeProvider.System,
                ShutdownTimeProvider = TimeProvider.System,
                BudgetTimeProvider = TimeProvider.System,
                Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
                Applier = new ReplicaGroupApplier(local, NullLogger.Instance, GroupId, "n2"),
            };
            for (var i = 1; i <= 3; i++)
                await CommitAsync($"k{i}", null, cancellationToken);

            await _committer.FlushAppliedAsync(Durable(), cancellationToken);
            _ = _registry.TryGetLog(GroupId, out var leaderLog);
            Compacted = await leaderLog!.CompactThroughAsync((await leaderLog.GetStatusAsync(cancellationToken)).CommitIndex, cancellationToken);
            await CommitAsync("k4", null, cancellationToken);
            Gateway.FollowerDown = false;
            Gateway.HoldEntries = true;
            var locator = new INodeLocatorCreateExpectations();
            _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(GroupId);
            _transfers = new ReplicaStateTransfers(Gateway, new ReplicaGroupEntries(Cache, locator.Instance()), Durable(), new ReplicaTransferOptions(), NullLogger<ReplicaStateTransfers>.Instance, null);
            _ = await _committer.VerifyReplicasAsync(cancellationToken);
        }

        internal Task CommitAsync(string key, DateTime? expiresUtc, CancellationToken cancellationToken) =>
            Committer.CommitSetAsync(NewOperationId(), CacheName, key, new NodeCacheEntry<object?>(key, 1, expiresUtc), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        /// <summary>Starts a catch-up pass of the followers, which repairs the follower n1 by a transfer.</summary>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>The pass, which completes with whether the follower was admitted.</returns>
        internal Task<bool> StartPassAsync(CancellationToken cancellationToken) =>
            Committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter(GroupId, NullLogger.Instance, null), _transfers, cancellationToken);

        private static T ThrowNotOpen<T>() => throw new InvalidOperationException("The leader is not open.");

        private static IJournalDurabilityCoordinator Durable()
        {
            var durability = new IJournalDurabilityCoordinatorCreateExpectations();
            _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
            return durability.Instance();
        }
    }

    /// <summary>The members of the group: the leader n2, the repaired follower n1 and a healthy follower n3.</summary>
    private sealed class Members : IReplicaGroupLocator
    {
        public int ReplicaCount => 3;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            destination[0] = "n2";
            destination[1] = "n1";
            destination[2] = "n3";
        }
    }

    /// <summary>Routes follower appends and transfer messages of node n1 to the real receiver, answers n3 as a healthy follower, and can hold the first entries message.</summary>
    private sealed class RoutedGateway : IReplicaRpcGateway, IReplicaTransferGateway
    {
        private readonly TaskCompletionSource _entriesHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _entriesRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ReplicaStateHarness _harness;

        internal RoutedGateway(ReplicaStateHarness harness)
        {
            _harness = harness;
        }

        internal Task EntriesHeld => _entriesHeld.Task;

        internal bool FollowerDown { get; set; }

        internal bool HoldEntries { get; set; }

        public Task<ReplicaTransferReply> AbortAsync(string nodeId, ReplicaRpcHeader header, string transferId, CancellationToken cancellationToken) =>
            _harness.Receiver.AbortAsync(SenderOf(in header, transferId), cancellationToken);

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var records = batch.Entries;
            var last = records.Count == 0 ? batch.PrevLogIndex : records[^1].LogIndex;
            if (!string.Equals(nodeId, "n1", StringComparison.Ordinal))
                return new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last);

            if (FollowerDown)
                throw new IOException("follower n1 is down");

            _ = _harness.Registry.TryGetLog(GroupId, out var log);
            var request = new FollowerLogAppendRequest(batch.LeaderNodeId, batch.LeaderTerm, batch.PrevLogIndex, batch.PrevLogTerm, batch.LeaderCommitIndex, new ReadOnlyMemory<FollowerLogEntry>([.. records]));
            return await log!.AppendAsync(request, cancellationToken).ConfigureAwait(false);
        }

        public Task<ReplicaTransferReply> BeginAsync(string nodeId, ReplicaRpcHeader header, string transferId, ReplicaTransferBaseline baseline, CancellationToken cancellationToken) =>
            _harness.Receiver.BeginAsync(SenderOf(in header, transferId), baseline, cancellationToken);

        public async Task<ReplicaTransferReply> SendAsync(string nodeId, ReplicaRpcHeader header, ReplicaTransferMessage message, CancellationToken cancellationToken)
        {
            var receiver = _harness.Receiver;
            switch (message.Step)
            {
                case ReplicaTransferStep.Snapshot:
                    return await receiver.SnapshotAsync(SenderOf(in header, message.TransferId), message.Sequence, message.Offset, message.Chunk, cancellationToken).ConfigureAwait(false);
                case ReplicaTransferStep.Wipe:
                    return await receiver.WipeAsync(SenderOf(in header, message.TransferId), message.Sequence, cancellationToken).ConfigureAwait(false);
                case ReplicaTransferStep.Entries:
                    if (!HoldEntries)
                        return await receiver.EntriesAsync(SenderOf(in header, message.TransferId), message.Sequence, message.Entries, cancellationToken).ConfigureAwait(false);
                    HoldEntries = false;
                    _ = _entriesHeld.TrySetResult();
                    await _entriesRelease.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                    return await receiver.EntriesAsync(SenderOf(in header, message.TransferId), message.Sequence, message.Entries, cancellationToken).ConfigureAwait(false);
                case ReplicaTransferStep.End:
                    return await receiver.EndAsync(SenderOf(in header, message.TransferId), message.Sequence, message.EntryCount, message.EntriesCrc, cancellationToken).ConfigureAwait(false);
                default:
                    throw new ArgumentOutOfRangeException(nameof(message), message.Step, "Unsupported transfer step.");
            }
        }

        internal void ReleaseEntries() => _ = _entriesRelease.TrySetResult();

        private static ReplicaTransferHeader SenderOf(in ReplicaRpcHeader header, string transferId) => new(header.GroupId, transferId, header.LeaderNodeId, header.Term);
    }
}
