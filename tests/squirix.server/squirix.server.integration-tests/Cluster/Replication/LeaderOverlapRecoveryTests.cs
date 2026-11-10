using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Enums;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Recovery after replicated entries lose their former leader's unflushed local copy.</summary>
[ExecutionPriority(Priority.Critical)]
public sealed class LeaderOverlapRecoveryTests : NodeIntegrationTestBase
{
    private const string Cache = "overlap-recovery";
    private const string Group = "node-a";
    private const string PrefixCache = "overlap-recovery-prefix";
    private const string Scope = "leader-overlap-recovery";
    private static readonly string[] Nodes = [Group, "node-b", "node-c"];
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>Recovers an acknowledged entry with a stale commit watermark after a cold restart.</summary>
    /// <param name="token">The test cancellation token.</param>
    [Test]
    public async Task AckSurvivesStaleCommitMetadata(CancellationToken token)
    {
        using var a = new HeldFlush();
        using var b = new HeldFlush();
        using var c = new HeldFlush();
        var faults = new Dictionary<string, HeldFlush>(StringComparer.Ordinal) { [Nodes[0]] = a, [Nodes[1]] = b, [Nodes[2]] = c };
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options(true, faults), token);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group);
        var (former, term) = await ElectLeaderAsync(cluster, ledger, faults, token);
        var leader = cluster[former];
        await StopPeriodicCompactionAsync(leader, token);
        var survivors = Array.FindAll(Nodes, node => !string.Equals(node, former, StringComparison.Ordinal));
        var key = leader.FindKeyOwnedBy(Cache, Group);
        var prefixKey = leader.FindKeyOwnedBy(PrefixCache, Group);
        await LedgerWrites.CommitAsync(ledger, leader, PrefixCache, prefixKey, token, value: "acknowledged");
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(leader, Group, [(survivors[0], cluster[survivors[0]]), (survivors[1], cluster[survivors[1]])], token);
        var index = (await Log(leader).GetStatusAsync(token)).LastLogIndex + 1;
        string[] writers = [former, survivors[0]];
        var checkpoint = await StaleCommitDiskImage.CaptureAsync(cluster, writers, Group, term, index, token);
        await cluster.StopNodeAsync(survivors[1]);
        var operation = Guid.NewGuid().ToString("N");

        _ = await Assert.That(await leader.GetRequiredService<ReplicaGroupCommitters>().For(Group).CommitTryAddAsync(operation, Cache, key, Entry(), token)).IsTrue();
        await StaleCommitDiskImage.AssertStaleAsync(checkpoint[0].Path, index, token);
        foreach (var node in writers)
            _ = await Assert.That(await CountOperationAsync(cluster[node], operation, token)).IsEqualTo(1);

        await StaleCommitDiskImage.CrashAndRestoreAsync(cluster, writers, checkpoint, index, token);
        foreach (var node in survivors)
            _ = await cluster.StartNodeAsync(node, Options(false, null), token);
        var (next, nextTerm) = await ledger.LeaderAsync(survivors, term, "survivors elect a new leader with stale metadata", token);
        _ = await Assert.That(next).IsEqualTo(survivors[0]);

        // Read before waiting for the target commit index, so an early stale read fails the test.
        _ = await Assert.That(await FencedReads.ReadAsync(ledger, cluster[next], Cache, key, token)).IsEqualTo("unflushed");
        await VerifyRecoveredAsync(ledger, cluster[next], index, operation, key, prefixKey, token);
        await StaleCommitDiskImage.AssertCheckpointAsync(cluster[next], Group, nextTerm, index + 1, token);
        await VerifyRetryAsync(ledger, cluster[next], operation, key, token);
        _ = await Assert.That(await cluster[next].GetRequiredService<ReplicaGroupCommitters>().For(Group).CommitTryAddAsync(Guid.NewGuid().ToString("N"), Cache, key, Entry(), token)).IsFalse();
        await RejoinAsync(cluster, ledger, former, index, operation, key, token);
        await VerifyDeferredCompactionAsync(cluster, ledger, next, nextTerm, token);
    }

    /// <summary>
    /// Both followers retain an entry whose leader failed before fsync. After losing only that leader's unsynced suffix,
    /// a new elected leader applies the entry, repairs the former leader, and replays a retry without a second record.
    /// </summary>
    /// <param name="holdCatchUp">Whether to keep a follower sender leased while the fault-injection precondition is checked.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UnflushedEntrySurvivesLeaderLoss(bool holdCatchUp, CancellationToken cancellationToken)
    {
        using var a = new HeldFlush();
        using var b = new HeldFlush();
        using var c = new HeldFlush();
        var faults = new Dictionary<string, HeldFlush>(StringComparer.Ordinal) { [Nodes[0]] = a, [Nodes[1]] = b, [Nodes[2]] = c };
        await using var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options(true, faults), cancellationToken);
        foreach (var group in Nodes)
            _ = await new GroupAuthorityLedger<IntegrationStartOptions>(cluster, group).LeaderAsync(Nodes, 0, "each group has authority", cancellationToken);

        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group);
        var (former, term) = await ElectLeaderAsync(cluster, ledger, faults, cancellationToken);
        var leader = cluster[former];
        var key = leader.FindKeyOwnedBy(Cache, Group);
        var prefixKey = leader.FindKeyOwnedBy(PrefixCache, Group);
        await LedgerWrites.CommitAsync(ledger, leader, PrefixCache, prefixKey, cancellationToken, value: "acknowledged");
        var survivors = Array.FindAll(Nodes, node => !string.Equals(node, former, StringComparison.Ordinal));
        await PrepareLiveSendersAsync(ledger, leader, [(survivors[0], cluster[survivors[0]]), (survivors[1], cluster[survivors[1]])], holdCatchUp, cancellationToken);
        var previous = await Log(leader).GetStatusAsync(cancellationToken);
        var index = previous.LastLogIndex + 1;
        var prefix = LogPrefix.Capture(leader.DataDir);
        var operation = Guid.NewGuid().ToString("N");
        var hook = faults[former];
        hook.Arm();
        var writing = leader.GetRequiredService<ReplicaGroupCommitters>().For(Group).CommitTryAddAsync(operation, Cache, key, Entry(), cancellationToken);
        try
        {
            await hook.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);
            foreach (var node in survivors)
                await AwaitUncommittedAsync(ledger, cluster[node], index, operation, cancellationToken);

            _ = await Assert.That(writing.IsCompleted).IsFalse();
            prefix.VerifyUnflushed(index, term, operation);
            await FailLocalWriteAsync(hook, writing, leader, previous.CommitIndex, cancellationToken);

            await leader.AbruptShutdownAsync();
            await cluster.StopNodeAsync(former);

            // The host shares this process's page cache. Explicit suffix loss models a permitted power-loss outcome;
            // AbruptShutdownAsync alone is not an OS process kill and cannot demonstrate loss of unflushed bytes.
            prefix.LoseUnflushed(index, term, operation);

            var (next, nextTerm) = await ledger.LeaderAsync(survivors, term, "survivors elect a new leader", cancellationToken);
            _ = await Assert.That(nextTerm).IsGreaterThan(term);
            await VerifyRecoveredAsync(ledger, cluster[next], index, operation, key, prefixKey, cancellationToken);
            await RejoinAsync(cluster, ledger, former, index, operation, key, cancellationToken);
            await VerifyRetryAsync(ledger, cluster[next], operation, key, cancellationToken);
        }
        finally
        {
            hook.Fail();
            Task completion = writing;
            await completion.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    private static Task AwaitCommittedAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost node, ulong index, string operation, CancellationToken token) =>
        ledger.UntilValueAsync(
            (Node: node, Index: index, Operation: operation),
            static async (s, ct) =>
        {
            var log = Log(s.Node);
            var status = await log.GetStatusAsync(ct);
            if (status.CommitIndex < s.Index || status.LastAppliedIndex < s.Index)
                return false;
            var entries = await log.ReadEntriesAsync(s.Index, 1, ct);
            var retention = await log.GetRetentionAsync(ct);
            _ = await Assert.That(retention.SnapshotIndex).IsLessThan(s.Index);
            _ = await Assert.That(entries.Entries[0].LogIndex).IsEqualTo(s.Index);
            _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(entries.Entries[0].Payload)?.OperationId).IsEqualTo(s.Operation);
            return true;
        },
            "the target is committed and durably applied",
            token);

    private static Task AwaitLiveSendersAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ReplicaGroupCommitter committer, List<ReplicaFollowerSender> senders, CancellationToken token) =>
        ledger.UntilAsync(
            () =>
            {
                if (!committer.Registry.EligibilityFor(Group).AllCanCountInWriteQuorum())
                    return false;

                var ready = true;
                for (var index = 0; index < senders.Count; index++)
                    ready &= senders[index].TryEnqueueHeartbeat(committer.Coordinator!.CommitIndex);
                return ready;
            },
            "both follower senders can send live appends",
            token);

    private static Task AwaitUncommittedAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost node, ulong index, string operation, CancellationToken token) =>
        ledger.UntilValueAsync(
            (Node: node, Index: index, Operation: operation),
            static async (s, ct) =>
        {
            var log = Log(s.Node);
            var status = await log.GetStatusAsync(ct);
            if (status.LastLogIndex < s.Index)
                return false;
            var entries = await log.ReadEntriesAsync(s.Index, 1, ct);
            _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(entries.Entries[0].Payload)?.OperationId).IsEqualTo(s.Operation);
            _ = await Assert.That(status.CommitIndex).IsLessThan(s.Index);
            return true;
        },
            "a follower durably holds the uncommitted target",
            token);

    private static async Task<int> CountOperationAsync(ITestNodeHost node, string operation, CancellationToken token)
    {
        var log = Log(node);
        var status = await log.GetStatusAsync(token);
        var entries = await log.ReadEntriesAsync(1, int.CreateChecked(status.LastLogIndex), token);
        var count = 0;
        foreach (var entry in entries.Entries)
        {
            if (string.Equals(ReplicaLogCodec.DecodeBorrowed(entry.Payload)?.OperationId, operation, StringComparison.Ordinal))
                count++;
        }

        return count;
    }

    private static ReplicaGroupRegistry CreateRegistry(IServiceProvider services, IReadOnlyDictionary<string, HeldFlush> faults)
    {
        var topology = services.GetRequiredService<TopologyOptions>();
        var peers = new string[topology.Peers.Count];
        for (var i = 0; i < peers.Length; i++)
            peers[i] = topology.Peers[i].NodeId;
        var clock = services.GetRequiredService<TimeProvider>();
        return new ReplicaGroupRegistry(
            services.GetRequiredService<PersistenceOptions>().DataDir,
            ReplicaGroupMembership.GroupsServedBy(services.GetRequiredService<IReplicaGroupLocator>(), peers, topology.NodeId),
            topology.ReplicaCount,
            BufferEx.CopyToOwned(TopologyFingerprint.CreateFromTopology(topology, services.GetRequiredService<MtlsOptions>()).Bytes),
            topology.ConfigurationGeneration,
            services.GetRequiredService<ILoggerFactory>(),
            new FollowerLogOptions
            {
                FaultHooks = faults[topology.NodeId],
                TimeProvider = clock,
                ShutdownTimeProvider = TimeProvider.System,
                IdempotencyCapacity = PolicyOptions.RfIdempotencyMaxInFlightRecords,
                IdempotencyRetention = TimeSpan.FromTicks(PolicyOptions.RfIdempotencyRetentionTicks),
                IdempotencyMinRetention = TimeSpan.FromTicks(PolicyOptions.RfIdempotencyMinRetentionTicks),
                IdempotencyMaxOutcomeBytes = PolicyOptions.RfIdempotencyMaxOutcomeBytes,
            })
        {
            Election = services.GetRequiredService<ElectionTimerOptions>(),
            ElectionClock = clock,
            QuorumReads = true,
        };
    }

    private static async Task<(string Leader, ulong Term)> ElectLeaderAsync(TestCluster<IntegrationStartOptions> cluster, GroupAuthorityLedger<IntegrationStartOptions> ledger, IReadOnlyDictionary<string, HeldFlush> faults, CancellationToken token)
    {
        var (bootstrap, term) = await ledger.LeaderAsync(Nodes, 0, "bootstrap authority is ready", token);
        var node = cluster[bootstrap];
        await LedgerWrites.CommitAsync(ledger, node, PrefixCache, node.FindKeyOwnedBy(PrefixCache, Group), token, value: "bootstrap");
        await cluster.StopNodeAsync(bootstrap);
        var survivors = Array.FindAll(Nodes, id => !string.Equals(id, bootstrap, StringComparison.Ordinal));
        _ = await ledger.LeaderAsync(survivors, term, "a leader wins an election before overlap is tested", token);
        _ = await cluster.StartNodeAsync(bootstrap, Options(false, faults), token);
        var (leader, electedTerm) = await ledger.LeaderAsync(Nodes, term, "the tested group has elected authority", token);
        _ = await Assert.That(electedTerm).IsGreaterThan(1UL);
        return (leader, electedTerm);
    }

    private static NodeCacheEntry<object?> Entry() => new() { Value = "unflushed", Version = 1 };

    private static async Task FailLocalWriteAsync(HeldFlush hook, Task<bool> writing, ITestNodeHost leader, ulong previousCommit, CancellationToken token)
    {
        hook.Fail();
        var failure = await NodeAsyncAssert.ThrowsAsync<SquirixException, bool>(new ValueTask<bool>(writing), Bound, TimeProvider.System, token);
        _ = await Assert.That(failure.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        await hook.Exited.WaitAsync(Bound, TimeProvider.System, token);
        _ = await Assert.That(Log(leader).Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await Log(leader).GetStatusAsync(token)).CommitIndex).IsEqualTo(previousCommit);
    }

    private static IFollowerLog Log(ITestNodeHost node) => node.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(Group, out var log)
        ? log
        : throw new InvalidOperationException("The tested group is not open.");

    private static IntegrationStartOptions Options(bool clean, IReadOnlyDictionary<string, HeldFlush>? faults) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = Scope,
        AutomaticFailoverEnabled = true,
        QuorumReadsEnabled = true,
        ElectionTiming = new TestElectionTiming(),

        ServicesConfigure = faults == null
            ? null
            : services =>
            {
                _ = services.RemoveAll<ReplicaGroupRegistry>();
                _ = services.AddSingleton(sp => CreateRegistry(sp, faults));
            },
    };

    private static async Task PrepareLiveSendersAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost leader, (string Id, ITestNodeHost Host)[] followers, bool holdCatchUp, CancellationToken token)
    {
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(leader, Group, followers, token);
        var committer = leader.GetRequiredService<ReplicaGroupCommitters>().For(Group);
        var pipeline = ThrowHelper.Required(committer.RunningPipeline, "The elected leader has no running pipeline.");
        var (members, _) = committer.Probe.BuildMembership(pipeline.Term);
        var senders = new List<ReplicaFollowerSender>();
        for (var slot = 0; slot < members.Length; slot++)
        {
            if (slot != committer.Probe.LeaderReplicaIndex)
                senders.Add(pipeline.CatchUpTargetFor(slot).Sender);
        }

        // Catch-up can hold a sender after its follower has received the prefix. Before holding the local log gate,
        // wait for both live senders: catch-up needs that gate to finish and would otherwise keep the target queued.
        await AwaitLiveSendersAsync(ledger, committer, senders, token);
        if (!holdCatchUp)
            return;

        using var lease = await senders[0].BeginCatchUpAsync(token);
        var preparing = AwaitLiveSendersAsync(ledger, committer, senders, token);
        try
        {
            _ = await Assert.That(preparing.IsCompleted).IsFalse();
        }
        finally
        {
            lease.Dispose();
        }

        await preparing;
    }

    private static async Task RejoinAsync(TestCluster<IntegrationStartOptions> cluster, GroupAuthorityLedger<IntegrationStartOptions> ledger, string former, ulong index, string operation, string key, CancellationToken token)
    {
        var returned = await cluster.StartNodeAsync(former, Options(false, null), token);
        await AwaitCommittedAsync(ledger, returned, index, operation, token);
        _ = await Assert.That(returned.GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(Group, out _)).IsFalse();
        var local = returned.Services.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey);
        _ = await Assert.That((await local.GetEntryAsync(Cache, key, token))?.Value).IsEqualTo("unflushed");
    }

    private static async Task StopPeriodicCompactionAsync(ITestNodeHost node, CancellationToken token)
    {
        foreach (var service in node.Services.GetServices<IHostedService>())
        {
            if (service is not ReplicaLogCompactionService compaction)
                continue;
            await compaction.StopAsync(token);
            return;
        }

        throw new InvalidOperationException("Replica log compaction is not registered.");
    }

    private static async Task VerifyDeferredCompactionAsync(TestCluster<IntegrationStartOptions> cluster, GroupAuthorityLedger<IntegrationStartOptions> ledger, string leaderId, ulong term, CancellationToken token)
    {
        const string cache = "deferred-compaction";
        var leader = cluster[leaderId];
        await StopPeriodicCompactionAsync(leader, token);
        var key = leader.FindKeyOwnedBy(cache, Group);
        await LedgerWrites.CommitAsync(ledger, leader, cache, key, token, value: "compacted");
        var target = (await Log(leader).GetStatusAsync(token)).CommitIndex;
        var path = GroupStoragePaths.GetMetadataPath(leader.DataDir, Group);
        await StaleCommitDiskImage.AssertStaleAsync(path, target, token);
        await StaleCommitDiskImage.CompactAsync(ledger, leader, Group, token);
        _ = await Assert.That((await Log(leader).GetRetentionAsync(token)).SnapshotIndex).IsGreaterThanOrEqualTo(target);
        await StaleCommitDiskImage.AssertCheckpointAsync(leader, Group, term, target, token);
        _ = await Assert.That(await FencedReads.ReadAsync(ledger, leader, cache, key, token)).IsEqualTo("compacted");
    }

    private static async Task VerifyRecoveredAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost node, ulong index, string operation, string key, string prefixKey, CancellationToken token)
    {
        await AwaitCommittedAsync(ledger, node, index, operation, token);
        _ = await Assert.That(await FencedReads.ReadAsync(ledger, node, Cache, key, token)).IsEqualTo("unflushed");
        _ = await Assert.That(await FencedReads.ReadAsync(ledger, node, PrefixCache, prefixKey, token)).IsEqualTo("acknowledged");
    }

    private static async Task VerifyRetryAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost node, string operation, string key, CancellationToken token)
    {
        var before = await CountOperationAsync(node, operation, token);
        var retried = await node.GetRequiredService<ReplicaGroupCommitters>().For(Group).CommitTryAddAsync(operation, Cache, key, Entry(), token);
        _ = await Assert.That(retried).IsTrue();
        _ = await Assert.That((before, await CountOperationAsync(node, operation, token))).IsEqualTo((1, 1));
        _ = await Assert.That(await FencedReads.ReadAsync(ledger, node, Cache, key, token)).IsEqualTo("unflushed");
    }

    private sealed class HeldFlush : IFollowerLogFaultHooks, IDisposable
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _exited = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _release = new(false);
        private int _armed;

        internal Task Entered => _entered.Task;

        internal Task Exited => _exited.Task;

        public void Dispose() => _release.Dispose();

        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
            if (Interlocked.Exchange(ref _armed, 0) == 0)
                return;
            _ = _entered.TrySetResult();
            try
            {
                _release.Wait(CancellationToken.None);
                throw new IOException("Injected failure before the local frame flush.");
            }
            finally
            {
                _ = _exited.TrySetResult();
            }
        }

        public void OnFramesScheduling()
        {
        }

        public void OnMetaPublishing()
        {
        }

        public void OnSnapshotPublishing()
        {
        }

        internal void Arm() => Volatile.Write(ref _armed, 1);

        internal void Fail() => _release.Set();
    }

    private sealed class LogPrefix
    {
        private readonly string _directory;
        private readonly bool[] _exists;
        private readonly byte[][] _files;
        private readonly (int Slot, ulong Generation) _selected;

        private LogPrefix(string directory, byte[][] files, (int Slot, ulong Generation) selected)
        {
            _directory = directory;
            _files = files;
            _selected = selected;
            _exists = [File.Exists(Path.Join(directory, "group.log.0")), File.Exists(Path.Join(directory, "group.log.1"))];
        }

        internal static LogPrefix Capture(string root)
        {
            var directory = GroupStoragePaths.GetGroupDirectory(root, Group);
            var files = ReadSlots(directory);
            return new LogPrefix(directory, files, Select(directory, files));
        }

        internal void LoseUnflushed(ulong index, ulong term, string operation)
        {
            VerifyUnflushed(index, term, operation);
            var path = Path.Join(_directory, $"group.log.{_selected.Slot}");
            using (var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Write, FileShare.None))
            {
                RandomAccess.SetLength(handle, _files[_selected.Slot].Length);
                RandomAccess.FlushToDisk(handle);
            }

            if (!ReadSlots(_directory)[_selected.Slot].AsSpan().SequenceEqual(_files[_selected.Slot]))
                throw new InvalidDataException("The acknowledged prefix was not preserved exactly.");
        }

        internal void VerifyUnflushed(ulong index, ulong term, string operation)
        {
            for (var slot = 0; slot < GroupLogSlots.SlotCount; slot++)
            {
                if (File.Exists(Path.Join(_directory, $"group.log.{slot}")) != _exists[slot])
                    throw new InvalidDataException("A log slot appeared or disappeared during fault injection.");
            }

            var files = ReadSlots(_directory);
            if (Select(_directory, files) != _selected || !files[1 - _selected.Slot].AsSpan().SequenceEqual(_files[1 - _selected.Slot]))
                throw new InvalidDataException("The log generation changed during fault injection.");
            var before = _files[_selected.Slot];
            var current = files[_selected.Slot];
            if (current.Length <= before.Length || !current.AsSpan(0, before.Length).SequenceEqual(before) ||
                !GroupLogCodec.TryReadFrame(current.AsSpan(before.Length), out var entry) || entry.LogIndex != index || entry.Term != term ||
                GroupLogCodec.ComputeFrameEncodedLength(entry.Payload.Length) != current.Length - before.Length ||
                !string.Equals(ReplicaLogCodec.DecodeBorrowed(entry.Payload)?.OperationId, operation, StringComparison.Ordinal))
                throw new InvalidDataException("The held flush does not contain exactly the target after the durable prefix.");
        }

        private static byte[] Read(string path)
        {
            if (!File.Exists(path))
                return [];
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var bytes = new byte[int.CreateChecked(RandomAccess.GetLength(handle))];
            var read = 0;
            while (read < bytes.Length)
            {
                var count = RandomAccess.Read(handle, bytes.AsSpan(read), read);
                if (count == 0)
                    throw new EndOfStreamException("The captured log shortened while it was read.");
                read += count;
            }

            return bytes;
        }

        private static byte[][] ReadSlots(string directory) => [Read(Path.Join(directory, "group.log.0")), Read(Path.Join(directory, "group.log.1"))];

        private static (int Slot, ulong Generation) Select(string directory, byte[][] files) => GroupLogSlots.Select(
            files[0].AsSpan(0, Math.Min(files[0].Length, GroupLogSlots.HeaderSize)),
            files[0].Length == 0 ? -1 : files[0].Length,
            files[1].AsSpan(0, Math.Min(files[1].Length, GroupLogSlots.HeaderSize)),
            files[1].Length == 0 ? -1 : files[1].Length,
            directory);
    }
}
