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
    public async Task FollowerEndsWithTheCacheOfTheLeader(CancellationToken cancellationToken)
    {
        using var followerDir = new TempDirectory("squirix-e2e-follower");
        using var leaderDir = new TempDirectory("squirix-e2e-leader");
        await using var harness = await ReplicaStateHarness.OpenAsync(followerDir, cancellationToken);
        await harness.SeedAsync(cancellationToken, "stale-1");
        await SeededLogs.SeedGroupsAsync(leaderDir, [GroupId], cancellationToken);
        await using var registry = await OpenLeaderRegistryAsync(leaderDir, cancellationToken);
        var leaderCache = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(leaderCache, leaderCache);
        var gateway = new RoutedGateway(harness) { FollowerDown = true };
        await using var committer = new ReplicaGroupCommitter(registry, new Members(), gateway, local, (GroupId, "n2"), new ReplicaTopologyStamp(TransferFixtures.Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Clock = TimeProvider.System,
            ShutdownTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, GroupId, "n2"),
        };
        for (var i = 1; i <= 3; i++)
            await CommitAsync(committer, $"k{i}", cancellationToken);

        await committer.FlushAppliedAsync(Durable(), cancellationToken);
        _ = registry.TryGetLog(GroupId, out var leaderLog);
        var compacted = await leaderLog!.CompactThroughAsync((await leaderLog.GetStatusAsync(cancellationToken)).CommitIndex, cancellationToken);
        await CommitAsync(committer, "k4", cancellationToken);
        gateway.FollowerDown = false;
        gateway.HoldEntries = true;
        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(GroupId);
        var transfers = new ReplicaStateTransfers(gateway, new ReplicaGroupEntries(leaderCache, locator.Instance()), Durable(), new ReplicaTransferOptions(), NullLogger<ReplicaStateTransfers>.Instance, null);
        _ = await committer.VerifyReplicasAsync(cancellationToken);

        var pass = committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter(GroupId, NullLogger.Instance, null), transfers, cancellationToken);
        await gateway.EntriesHeld.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await CommitAsync(committer, "k5", cancellationToken);
        gateway.ReleaseEntries();
        var admitted = await pass.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = harness.Registry.TryGetLog(GroupId, out var followerLog);
        var status = await followerLog!.GetStatusAsync(cancellationToken);
        await harness.Appliers.For(GroupId).CatchUpAsync(followerLog, status.LastAppliedIndex, status.CommitIndex, cancellationToken);
        _ = await Assert.That(compacted).IsEqualTo(GroupCompactionOutcome.Compacted);
        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(1)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(followerLog.IsComplete).IsTrue();
        _ = await Assert.That(await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "stale-1"), cancellationToken)).IsNull();
        for (var i = 1; i <= 5; i++)
        {
            var leaderEntry = await leaderCache.GetEntryAsync(new CacheKey(CacheName, $"k{i}"), cancellationToken);
            var followerEntry = await harness.Cache.GetEntryAsync(new CacheKey(CacheName, $"k{i}"), cancellationToken);
            _ = await Assert.That((followerEntry?.Value, followerEntry?.Version)).IsEqualTo((leaderEntry?.Value, leaderEntry?.Version));
            _ = await Assert.That(followerEntry).IsNotNull();
        }
    }

    private static Task CommitAsync(ReplicaGroupCommitter committer, string key, CancellationToken cancellationToken) =>
        committer.CommitSetAsync(NewOperationId(), CacheName, key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

    private static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    private static async Task<ReplicaGroupRegistry> OpenLeaderRegistryAsync(string dir, CancellationToken cancellationToken)
    {
        var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, TransferFixtures.Fingerprint, 1UL, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System };
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
            _harness.Receiver.AbortAsync(header.GroupId, transferId, cancellationToken);

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var records = batch.Records;
            var last = records.Count == 0 ? batch.PrevLogIndex : records[^1].LogIndex;
            if (!string.Equals(nodeId, "n1", StringComparison.Ordinal))
                return new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last);

            if (FollowerDown)
                throw new IOException("follower n1 is down");

            var entries = new FollowerLogEntry[records.Count];
            for (var i = 0; i < entries.Length; i++)
            {
                var record = records[i];
                entries[i] = new FollowerLogEntry(record.LogIndex, record.Term, ReplicaLogCodec.Encode(in record));
            }

            _ = _harness.Registry.TryGetLog(GroupId, out var log);
            var request = new FollowerLogAppendRequest(batch.LeaderNodeId, batch.LeaderTerm, batch.PrevLogIndex, batch.PrevLogTerm, batch.LeaderCommitIndex, entries);
            return await log!.AppendAsync(request, cancellationToken).ConfigureAwait(false);
        }

        public Task<ReplicaTransferReply> BeginAsync(string nodeId, ReplicaRpcHeader header, string transferId, ReplicaTransferBaseline baseline, CancellationToken cancellationToken) =>
            _harness.Receiver.BeginAsync(new ReplicaTransferHeader(header.GroupId, transferId, header.LeaderNodeId, header.Term), baseline, cancellationToken);

        public async Task<ReplicaTransferReply> SendAsync(string nodeId, ReplicaRpcHeader header, ReplicaTransferMessage message, CancellationToken cancellationToken)
        {
            var receiver = _harness.Receiver;
            switch (message.Step)
            {
                case ReplicaTransferStep.Snapshot:
                    return await receiver.SnapshotAsync(message.GroupId, message.TransferId, message.Sequence, message.Offset, message.Chunk, cancellationToken).ConfigureAwait(false);
                case ReplicaTransferStep.Wipe:
                    return await receiver.WipeAsync(message.GroupId, message.TransferId, message.Sequence, cancellationToken).ConfigureAwait(false);
                case ReplicaTransferStep.Entries:
                    if (HoldEntries)
                    {
                        HoldEntries = false;
                        _ = _entriesHeld.TrySetResult();
                        await _entriesRelease.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                    }

                    return await receiver.EntriesAsync(message.GroupId, message.TransferId, message.Sequence, message.Entries, cancellationToken).ConfigureAwait(false);
                case ReplicaTransferStep.End:
                    return await receiver.EndAsync(message.GroupId, message.TransferId, message.Sequence, message.EntryCount, message.EntriesCrc, cancellationToken).ConfigureAwait(false);
                default:
                    throw new ArgumentOutOfRangeException(nameof(message), message.Step, "Unsupported transfer step.");
            }
        }

        internal void ReleaseEntries() => _ = _entriesRelease.TrySetResult();
    }
}
